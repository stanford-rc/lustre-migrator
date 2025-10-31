# Lustre Migrator - Architectural Design

## 1. Overview

Lustre Migrator is built on a distributed, leader-follower architecture designed for simplicity, scalability, and fault tolerance. The entire system is orchestrated without a dedicated external message bus or database. Instead, it leverages a **shared filesystem** for leader election and an **embedded Redis server** on the leader node for state and job management.

The core principles are:
-   **No Single Point of Failure (SPOF)**: The leader is ephemeral, and any worker node can become the leader.
-   **Simplicity**: No complex external dependencies like Zookeeper or a managed Redis cluster are required. `redis-server` simply needs to be in the `PATH`.
-   **Scalability**: Performance can be increased linearly by simply starting more worker daemons on more nodes.

## 2. Core Components

The system is composed of three main logical components: the Shared Filesystem, the Leader Daemon, and one or more Follower Daemons.

### 2.1. Shared Filesystem (e.g., NFS)

This is the bedrock of the entire system. It must be a reliable, POSIX-compliant shared filesystem mounted identically on all participating nodes. Its responsibilities are:
-   **Leader Election**: Hosts a lock file (`redis.lock`) which daemons compete to lock using `fcntl.flock()`. The winner becomes the leader.
-   **Configuration Store**: Contains the per-campaign configuration (`config.json`) and the list of initial scan paths (`initial_scan_jobs.list`).
-   **Leader Discovery**: A file (`leader.info`) is written by the leader, containing its IP address and port, allowing followers to find it.
-   **Persistent Logging**: Stores compliance logs for successful and failed migrations.

### 2.2. The Leader Daemon

A `lustre-migratord` instance that successfully acquires the file lock becomes the leader. It has two primary responsibilities:
1.  **Run the Central Job Broker**: It starts and manages an **embedded `redis-server` process**. This Redis instance acts as the central nervous system for the campaign, holding job queues and real-time state. To enhance security, it is configured to require a password, which is automatically set to the campaign name.
2.  **Perform Work**: In addition to its leadership duties, the leader node also runs its own pool of scan and migration worker threads, contributing to the overall effort just like any follower.

### 2.3. The Follower Daemons

Any `lustre-migratord` instance that fails to acquire the lock becomes a follower. Its role is simple:
1.  **Find the Leader**: It reads the `leader.info` file from the shared filesystem to get the leader's IP address.
2.  **Connect and Work**: It connects to the leader's Redis server and begins pulling jobs from the `scan` and `migrate` queues to perform work.

## 3. Architecture Diagram

The following diagram illustrates the interaction between the components:

```ascii
                    +-------------------------------------------------------------+
                    |                           Network                           |
                    +-------------------------------------------------------------+
                                     ^                     ^
                                     |                     |
+------------------------------------|------------------+  |  +--------------------------------+
| Leader Node (node-01)              |                  |  |  | Follower Node (node-02)        |
|                                    | Redis TCP/IP     |  |  |                                |
|  +---------------------------+     | Connection       |  |  |  +---------------------------+ |
|  |   lustre-migratord PID 123|     +<--------------------)---+  |   lustre-migratord PID 456| |
|  |                           |                        |  |  |                           | |
|  | +-----------------------+ |                        |  |  | +-----------------------+ | |
|  | | Embedded redis-server | |                        |  |  | | Scan/Migrate Threads  | | |
|  | |-----------------------| |                        |  |  | |-----------------------| | |
|  | | - scan_queue          | |                        |  |  | | - lfs find            | | |
|  | | - migrate_queue       | |                        |  |  | | - lfs migrate         | | |
|  | | - sets: discovered    | |                        |  |  | | - ...                 | | |
|  | | - sets: succeeded     | |                        |  |  | +-----------------------+ | |
|  | | - ...                 | |                        |  |  |                           | |
|  | +-----------------------+ |                        |  |  |                           | |
|  +---------------------------+     +--------------------)---+  +---------------------------+ |
|                                    |                  |     |                                |
+-----------------|------------------+                  +-----|--------------------------------+
                  |                                          |
                  |  <---R/W--->                             |  <---R/W--->
                  |                                          |
+-----------------v------------------------------------------v---------------------------------+
|                                Shared Filesystem (NFS)                                     |
|                                                                                            |
|   /campaign_name/                                                                          |
|   |                                                                                        |
|   +-- [ redis.lock ] <--- Leader holds exclusive fcntl.flock(LOCK_EX)                       |
|   |                                                                                        |
|   +-- [ leader.info ] <--- Written by Leader, read by Followers (e.g., "10.0.16.51:6379")   |
|   |                                                                                        |
|   +-- [ config.json, initial_scan_jobs.list, ... ]                                         |
|                                                                                            |
+--------------------------------------------------------------------------------------------+
```

## 4. Detailed Workflow (Data Flow)

1.  **Campaign Start**: An admin runs `lustre-migrator start`, creating the campaign directory and configuration files on the shared filesystem.
2.  **Daemon Initialization**: `systemctl start lustre-migratord@campaign` is run on multiple nodes.
3.  **Leader Election**: All daemons attempt to acquire an exclusive, non-blocking lock on `redis.lock`. Only one succeeds.
4.  **Leader Setup**:
    -   The winning daemon starts the `redis-server` process, binding it to the IP on the configured `communication_subnet`.
    -   It writes its IP and port to `leader.info`.
    -   It checks for `initial_scan_jobs.list` and, if present, pushes the paths into the `queues:scan` list in Redis.
5.  **Follower Setup**:
    -   Daemons that failed to get the lock read `leader.info`.
    -   They connect to the leader's Redis instance.
6.  **Scanning Phase**:
    -   A worker thread on any node (leader or follower) performs a blocking pop (`BLPOP`) on the `queues:scan` queue.
    -   Upon receiving a directory path, it executes `lfs find` to discover files on the target OSTs.
    -   It batches the discovered file paths and pushes them to two places in Redis:
        -   The `sets:discovered` set (for progress tracking).
        -   The `queues:migrate` list (for the next phase).
7.  **Migration Phase**:
    -   A worker thread on any node performs a `BLPOP` on the `queues:migrate` queue.
    -   Upon receiving a file path, it executes `lfs migrate` on that file.
    -   Based on the result, it adds the file path to either the `sets:succeeded` or `sets:failed` set in Redis.

This process continues until all queues (`scan` and `migrate`) are empty and no jobs are in progress.

## 5. Fault Tolerance & High Availability

The system is designed to automatically recover from a leader failure.

**Scenario**: The leader node crashes or becomes network-isolated.

1.  **Detection**:
    -   The OS automatically releases the `fcntl` lock on `redis.lock` when the leader's process dies.
    -   The follower daemons lose their TCP connection to the leader's Redis server.
2.  **Follower Response**:
    -   The active threads in the followers encounter a `redis.exceptions.ConnectionError`.
    -   This error is caught, and the main daemon process is instructed to shut down with a non-zero exit code.
3.  **Systemd Recovery**:
    -   The `lustre-migratord@.service` unit is configured with `Restart=on-failure`.
    -   `systemd` detects the non-zero exit code and automatically restarts the follower daemons after a short delay.
4.  **New Election**:
    -   The newly restarted daemons re-enter the leader election loop.
    -   One of them will successfully acquire the now-unlocked `redis.lock` and become the new leader.
5.  **State Restoration**:
    -   The new leader starts its own `redis-server`. Since Redis AOF persistence is enabled, most of the state (queues, sets) is recovered automatically from the AOF file on the shared disk.
    -   The new leader then performs a **recovery routine**: it inspects the `*:inprogress` hashes for any "stranded" jobs (jobs that the old workers were processing when they died) and pushes them back onto their respective queues. This ensures no work is lost.

This entire cycle is automatic and ensures the migration campaign can survive the loss of any single node, including the leader.
