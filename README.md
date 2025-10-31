# Lustre Migrator

**Lustre Migrator** is a distributed, fault-tolerant tool for migrating files off a specific set of Lustre OSTs (Object Storage Targets). It is designed to run across multiple migration nodes (lustre clients) to parallelize work, providing high-throughput data migration with real-time monitoring and operational controls.

This project is licensed under the GPL v3. Please see the `LICENSE` file for details.

## Key Features

-   **Distributed & Scalable**: Runs on multiple nodes simultaneously, distributing scanning and migration load across a cluster.
-   **Fault-Tolerant**: Implements a leader/follower architecture. If a leader node fails, another node automatically takes over, ensuring the campaign continues with minimal disruption.
-   **High Performance**: Leverages multi-threading for concurrent `lfs find` (scanning) and `lfs migrate` (migration) operations on each node.
-   **Real-time Monitoring**: A rich CLI status command provides a detailed, live view of the campaign's progress, including completion percentage, ETA, active workers, and in-progress files.
-   **API for Monitoring**: Each daemon exposes a `/metrics` endpoint for easy integration with telegraf, `curl`, or other monitoring systems.
-   **Operational Control**: Campaigns can be paused, resumed, and failed files can be retried with simple commands.
-   **Safe by Design**: A robust file-based locking mechanism on a shared filesystem ensures only one leader exists, preventing split-brain scenarios. A **dry-run mode** is available to test the complete migration workflow (discovery, coordination, metrics) without actually moving data, allowing you to validate configurations and measure coordination system capacity before production runs.


## Architecture

The system consists of a `lustre-migratord` daemon running on multiple nodes.

-   **Shared Filesystem**: All state, configuration, and logs for a migration "campaign" are stored on a shared filesystem (e.g., NFS) accessible by all participating nodes.
-   **Leader Election**: The first daemon to acquire an exclusive file lock in the campaign directory becomes the **leader**. It starts an embedded Redis server to manage job queues and state.
-   **Followers**: All other daemons become **followers**. They connect to the leader's Redis server to pull scanning and migration jobs.
-   **High Availability**: If the leader fails or the lock is lost, its Redis server shuts down. The followers detect the disconnect, and one will acquire the lock to become the new leader. The new leader recovers any "in-flight" jobs from the previous leader and resumes the campaign.
-   **Work-flow**:
    1.  A campaign is defined with a set of `scan_paths`.
    2.  The leader primes a Redis `scan` queue with these paths.
    3.  Workers pop paths from the `scan` queue and run `lfs find` to discover files on the target OSTs.
    4.  Discovered files are pushed to a Redis `migrate` queue.
    5.  Workers pop file paths from the `migrate` queue and execute `lfs migrate` on them.

---

### A Note on AI-Assisted Development

Please be aware that this project was designed and implemented in close collaboration with an AI assistant.
This modern development approach significantly accelerated the creation process and helped in exploring various architectural solutions.

While the project is functional and has been used in production, we strongly encourage users to:

-   **Thoroughly review the source code** to understand its logic and implementation details.
-   **Conduct comprehensive testing** in a non-critical environment before deploying for production use.

This notice serves as a transparent disclosure of the development methodology.

---

## Quick Start Guide

This guide walks through a simple campaign to migrate files off OSTs in the index range `10-20`. We assume installation and configuration are complete.

### Step 1: Create the Campaign

On any node, define the migration campaign. This command specifies the target OSTs and the directories to scan for files.

```bash
/opt/lustre-migrator/lustre-migrator start \
    --name ost-evac-1 \
    --osts 10-20 \
    --scan-path "/lustre/fs1/data"
```

### Step 2: Start the First Worker (Leader)

On your first worker node (`node-01`), start the daemon. This instance will become the **leader** and begin processing scan jobs.

```bash
# On node-01
systemctl start lustre-migratord@ost-evac-1
```

### Step 3: Monitor Initial Progress

Check the status. You will see one active worker (the leader) and progress beginning.

```bash
/opt/lustre-migrator/lustre-migrator status ost-evac-1

--- Campaign Status: ost-evac-1 ---
State: RUNNING | Leader: node-01
Active Workers: 1 (node-01)

Progress: 15,102 / 1,250,000 files migrated (1.21%)
[..............................] | ETA: 4:32:10
...
```

### Step 4: Add More Workers to Scale Up

Scaling is simple. To add more workers, just start the same service on other nodes. They will automatically find the leader and start pulling jobs.

```bash
# On node-02
systemctl start lustre-migratord@ost-evac-1

# On node-03
systemctl start lustre-migratord@ost-evac-1

# On node-04
systemctl start lustre-migratord@ost-evac-1
```

### Step 5: Observe the Scaled-Up Status

Check the status again. You'll see more active workers, a higher migration rate, and a lower ETA.

```bash
/opt/lustre-migrator/lustre-migrator status ost-evac-1

--- Campaign Status: ost-evac-1 ---
State: RUNNING | Leader: node-01
Active Workers: 4 (node-[01-04])

Progress: 450,831 / 1,250,000 files migrated (36.07%)
[█████████.....................] | ETA: 1:08:05
...
Activity:
  - Migrations In Progress (32) (Rate: 180 files/sec):
    - /lustre/fs1/data/sim/result_1.dat (on node-01:MigrateWorker-1)
    - /lustre/fs1/data/raw/img_234.tif (on node-02:MigrateWorker-5)
    ...
```

### Step 6: Complete the Campaign

The daemons will run until all queues are empty, then shut down cleanly. You can also stop them manually at any time on all nodes:

```bash
systemctl stop lustre-migratord@ost-evac-1
```

## Installation

### Prerequisites

-   Python 3.6+
-   `redis-server` (must be in the system's PATH)
-   A shared filesystem (e.g., NFS) mounted identically on all nodes.
-   Root access for running `systemd` services and `lfs migrate`.

### Python Dependencies

Install the required Python packages:

```bash
dnf install python3-click python3-redis python3-clustershell python3-netifaces python3-flask
```

### Application Files

1.  Clone or copy the project files to a location on your system, for example, `/opt/lustre-migrator/`.
2.  Ensure the executables have the correct permissions:
    ```bash
    chmod +x /opt/lustre-migrator/lustre-migrator
    chmod +x /opt/lustre-migrator/lustre-migratord
    ```

## Configuration

1.  **Main Configuration**: Copy `lustre-migrator.conf` to `/etc/lustre-migrator.conf` and customize it.

    ```ini
    # /etc/lustre-migrator.conf
    [main]
    # This path MUST be on a shared filesystem accessible by all nodes.
    base_path = /var/lib/lustre-migrator

    # IP subnet for daemon communication (leader election, Redis).
    # Crucial for multi-homed nodes.
    communication_subnet = 10.0.16.0/24
    
    # ... other settings ...
    ```
    Create the `base_path` directory:
    ```bash
    mkdir -p /var/lib/lustre-migrator
    chown root:root /var/lib/lustre-migrator
    ```

2.  **Systemd Service**: Copy `systemd/lustre-migratord@.service` to `/etc/systemd/system/`.

    -   **Important**: Edit `/etc/systemd/system/lustre-migratord@.service` and update the `ExecStart` line to point to the correct path of your `lustre-migratord` executable.
    
    ```systemd
    [Service]
    ...
    ExecStart=/opt/lustre-migrator/lustre-migratord --campaign %i --log-level INFO --scan-workers 4 --migration-workers 8
    ...
    ```
    
    Reload the systemd daemon:
    ```bash
    systemctl daemon-reload
    ```

## Detailed Usage & Commands

### `start`
Create a new migration campaign.

```bash
/opt/lustre-migrator/lustre-migrator start \
    --name my-migration-1 \
    --osts 192-271 \
    --scan-path "/lustre/fs1/project/group1" \
    --exclude-path "/lustre/fs1/project/group1/do-not-touch"
```

### `status`
Show the live progress of a campaign.

```bash
/opt/lustre-migrator/lustre-migrator status <campaign_name>
```

### `pause`
Gracefully pauses a campaign, waiting for in-progress jobs to finish.

```bash
/opt/lustre-migrator/lustre-migrator pause <campaign_name>
```

### `resume`
Resumes a paused campaign.

```bash
/opt/lustre-migrator/lustre-migrator resume <campaign_name>
```

### `retry-failed`
Moves all files from the failed set back into the migration queue for another attempt.

```bash
/opt/lustre-migrator/lustre-migrator retry-failed <campaign_name>
```

### `reprime`
Resets the initial scan jobs file to be re-processed by the leader. Useful if the leader crashed during the initial queue priming.

```bash
/opt/lustre-migrator/lustre-migrator reprime <campaign_name>
```

## Monitoring via API (JSON)

Each `lustre-migratord` instance runs a web server (default port `9188`). You can scrape the `/metrics` endpoint to get detailed local and global metrics in JSON format, suitable for ingestion into Prometheus or for quick checks with `curl`.

To query the metrics from a worker node and format the output with `jq`:

```bash
curl -s http://localhost:9188/metrics | jq
```

**Example Output:**
```json
{
  "global_metrics": {
    "active_workers": 4,
    "campaign_name": "ost-evac-1",
    "campaign_status": "running",
    "leader_hostname": "node-01",
    "progress": {
      "files_discovered": 1250000,
      "files_failed": 0,
      "files_in_progress": 0,
      "files_succeeded": 450831
    },
    "queues": {
      "migrate_jobs_pending": 799169,
      "scan_jobs_pending": 0
    },
    "target_osts": "10-20"
  },
  "local_metrics": {
    "counters": {
      "files_migrated_failed": 0,
      "files_migrated_succeeded": 112708,
      "scans_completed": 5
    },
    "hostname": "node-01",
    "rates": {
      "found_files_rate_per_sec": 0,
      "migrate_rate_files_per_sec": 45.2
    },
    "status": "migrating"
  }
}
```

## Contact info

This project is maintained and used in production by [Stanford Research Computing](https://srcc.stanford.edu/) on [Oak Storage](https://docs.oak.stanford.edu/).

Have questions? Feel free to contact us at srcc-support@stanford.edu

