# Copyright (C) 2025
#      The Board of Trustees of the Leland Stanford Junior University
# Written by Stephane Thiell <sthiell@stanford.edu>
#
# Licensed under GPL v3 (see https://www.gnu.org/licenses/).

import fcntl
import logging
import os
import signal
import subprocess
import threading
import time
from typing import List

import redis
import redis.exceptions

from .common import (
    APP_CONFIG, INITIAL_JOBS_FILE, LEADER_INFO_FILE, LOGS_DIR,
    REDIS_DATA_DIR, REDIS_LOCK_FILE, get_campaign_path, get_primary_ip,
    get_short_hostname, key, load_config, get_redis_connection
)
from .metrics import MetricsServer
from .worker import Worker


class Daemon:
    """
    Main daemon class that can operate as a leader or a follower.

    Manages leader election, Redis server lifecycle, and supervises separate
    thread pools for scanning and migration workers.
    """
    def __init__(
        self, campaign_name: str, log_level: str, dry_run: bool,
        num_scan_workers: int, num_migration_workers: int, max_pending_migrations: int
    ):
        self.campaign_name = campaign_name
        self.log_level = log_level
        self.dry_run = dry_run
        self.num_scan_workers = num_scan_workers
        self.num_migration_workers = num_migration_workers
        self.max_pending_migrations = max_pending_migrations

        self.is_leader = False
        self.advertise_address = get_primary_ip()
        self.hostname = get_short_hostname()
        self.campaign_path = get_campaign_path(self.campaign_name)
        self.lock_file_path = os.path.join(self.campaign_path, REDIS_LOCK_FILE)

        self.lock_file_handle = None
        self.redis_proc = None
        self.shutdown_flag = False
        self.last_lock_check = 0
        self.last_completion_check = 0

        self.worker_threads: List[threading.Thread] = []
        self.metrics_server: MetricsServer = None
        self.shutdown_due_to_error = False

        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)

    def _handle_shutdown(self, signum, frame):
        """Sets the shutdown flag when a signal is received."""
        if not self.shutdown_flag:
            logging.info("Shutdown signal received (%s). Cleaning up...", signum)
            self.shutdown_flag = True

    def _scan_thread_target(self, worker: Worker, r: redis.Redis):
        """The main loop for a thread dedicated to scanning."""
        thread_name = threading.current_thread().name
        logging.info("%s started and waiting for scan jobs.", thread_name)

        while not self.shutdown_flag:
            try:
                campaign_status = r.hget(
                    key(self.campaign_name, 'campaign:state'), 'status'
                )
                if campaign_status in (b'pausing', b'paused', b'completed'):
                    time.sleep(5)
                    continue

                # The call to run_scan_job() will now block internally if throttling is needed.
                if not worker.run_scan_job():
                    time.sleep(1)

            # The following block handles Redis connection errors, which typically
            # occur during a leader failover. The design here is to "let it crash".
            # The error triggers a daemon shutdown with a non-zero exit code.
            # Systemd, configured with `Restart=on-failure`, will then restart
            # the entire daemon process. The restarted process begins in a clean
            # state and will find the new leader. This is simpler and more robust
            # than implementing a complex in-process reconnection mechanism.
            except redis.exceptions.ConnectionError as e:
                if not self.shutdown_flag:
                    logging.error(
                        "[%s] Redis connection error: %s. Signaling daemon shutdown.",
                        thread_name, e
                    )
                    self.shutdown_due_to_error = True
                    self.shutdown_flag = True
                else:
                    logging.debug(
                        "[%s] Encountered expected connection error during shutdown.",
                        thread_name
                    )
                break
            except Exception as e:
                logging.error(
                    "[%s] Unexpected error: %s", thread_name, e, exc_info=True
                )
                time.sleep(5)
        logging.info("%s exiting.", thread_name)

    def _migrate_thread_target(self, worker: Worker, r: redis.Redis):
        """The main loop for a thread dedicated to migration."""
        thread_name = threading.current_thread().name
        logging.info("%s started and waiting for migration jobs.", thread_name)

        while not self.shutdown_flag:
            try:
                campaign_status = r.hget(
                    key(self.campaign_name, 'campaign:state'), 'status'
                )
                if campaign_status in (b'pausing', b'paused', b'completed'):
                    time.sleep(5)
                    continue

                if not worker.run_migrate_job():
                    time.sleep(1)
            except redis.exceptions.ConnectionError as e:
                if not self.shutdown_flag:
                    logging.error(
                        "[%s] Redis connection error: %s. Signaling daemon shutdown.",
                        thread_name, e
                    )
                    self.shutdown_due_to_error = True
                    self.shutdown_flag = True
                else:
                    logging.debug(
                        "[%s] Encountered expected connection error during shutdown.",
                        thread_name
                    )
                break
            except Exception as e:
                logging.error(
                    "[%s] Unexpected error: %s", thread_name, e, exc_info=True
                )
                time.sleep(5)
        logging.info("%s exiting.", thread_name)

    def _try_acquire_lock(self):
        """
        Atomically creates and locks a file to become leader.
        """
        # This logic now uses os.open with O_CREAT for atomic file creation
        # and locking, preventing a race condition where another process could
        # lock the file between our open() and flock() calls.
        fh = None
        try:
            # Atomically create the file if it doesn't exist.
            fd = os.open(self.lock_file_path, os.O_CREAT | os.O_WRONLY, 0o644)
            # fdopen takes ownership of the file descriptor.
            fh = os.fdopen(fd, 'w', encoding='utf-8')

            # Attempt to acquire an exclusive, non-blocking lock.
            fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)

            # Success: we are the leader. Write our info and keep the handle.
            fh.write(f"{self.hostname}:{os.getpid()}\n")
            fh.flush()
            self.lock_file_handle = fh
            self.is_leader = True
            logging.info("Successfully acquired leader lock.")
        except (IOError, OSError):
            # Failure: we are a follower or an error occurred.
            self.is_leader = False
            self.lock_file_handle = None
            # If the file handle was created, close it.
            if fh:
                fh.close()

    def _wait_for_redis(self, host='localhost', timeout=120):
        """Waits for a Redis server to become available."""
        logging.info("Waiting for Redis server at %s:%s...", host, APP_CONFIG.REDIS_PORT)
        start_time = time.time()
        while time.time() - start_time < timeout:
            if self.shutdown_flag:
                raise InterruptedError("Shutdown signaled while waiting for Redis.")
            try:
                r = redis.Redis(
                    host=host,
                    port=APP_CONFIG.REDIS_PORT,
                    db=0,
                    password=self.campaign_name,
                    decode_responses=False
                )
                r.ping()
                logging.info("Successfully connected to Redis at %s.", host)
                return r
            except redis.exceptions.AuthenticationError as e:
                # This is a fatal, non-recoverable error during startup.
                # It almost certainly means a stray redis-server is running.
                logging.critical(
                    "FATAL: Redis authentication failed: %s. This likely means "
                    "another redis-server is running on port %d. Please "
                    "find and kill the stray process.", e, APP_CONFIG.REDIS_PORT
                )
                # Re-raise to crash the daemon immediately.
                raise
            except redis.exceptions.ConnectionError:
                # This is a recoverable error, just wait and retry.
                logging.debug("Redis not ready, waiting 2s...")
                time.sleep(2)
        raise ConnectionError(
            f"Redis at {host}:{APP_CONFIG.REDIS_PORT} not ready after {timeout}s"
        )

    def _start_redis_server(self):
        """Starts the embedded Redis server process as the leader."""
        redis_data_path = os.path.join(self.campaign_path, REDIS_DATA_DIR)
        os.makedirs(redis_data_path, exist_ok=True)

        # Define a unique, local log file path for Redis to prevent stalls on
        # shared storage for logging operations.
        redis_logfile_path = f"/tmp/lustre-migrator-redis-{self.campaign_name}-{self.hostname}.log"

        # To provide a baseline of security and prevent accidental connections
        # from other processes on the network, we now use the campaign name as
        # the Redis password. This is a pragmatic compromise that adds security
        # with zero extra configuration for the administrator.
        cmd = [
            "redis-server",
            "--bind", self.advertise_address, "127.0.0.1",
            "--port", str(APP_CONFIG.REDIS_PORT),
            "--dir", redis_data_path,
            "--loglevel", "notice",
            "--logfile", redis_logfile_path,
            "--appendonly", "yes",
            # Set `appendfsync` to `no` to delegates fsync responsibility to the OS.
            # The risk is losing up to ~30s of data on a hard crash, which is
            # acceptable as the app will just re-work jobs.
            "--appendfsync", "no",
            # For automatic recovery from a crash.
            "--aof-load-truncated", "yes",
            "--requirepass", self.campaign_name,
        ]
        logging.info(
            "Starting Redis server as leader with AOF persistence in %s...",
            redis_data_path
        )
        # Log the location of the Redis log file for easier debugging.
        logging.info("Redis log file will be written to: %s", redis_logfile_path)
        self.redis_proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        with open(os.path.join(self.campaign_path, LEADER_INFO_FILE), 'w', encoding='utf-8') as f:
            f.write(f"{self.advertise_address}:{APP_CONFIG.REDIS_PORT}")

    def _prime_initial_jobs(self, r: redis.Redis):
        """If an initial jobs file exists, push its contents to the scan queue."""
        jobs_file_path = os.path.join(self.campaign_path, INITIAL_JOBS_FILE)
        if not os.path.exists(jobs_file_path):
            return

        logging.info("Found initial jobs file. Priming Redis queue.")
        try:
            with open(jobs_file_path, 'r', encoding='utf-8') as f:
                initial_jobs = [line.strip() for line in f if line.strip()]

            if not initial_jobs:
                logging.warning("Initial jobs file is empty. Nothing to prime.")
                return

            # To prevent a race condition where a crash could cause duplicate jobs,
            # we rename the file FIRST to atomically mark it as "in progress".
            # If the script crashes after the rename but before the Redis push,
            # the jobs won't be in the queue, but they also won't be pushed again
            # on the next start. This failure is easily recoverable by an admin
            # using the `reprime` command. This prioritizes correctness (no
            # duplicates) over automatic availability in a rare crash scenario.
            processed_file_path = jobs_file_path + ".processed"
            os.rename(jobs_file_path, processed_file_path)
            logging.info(
                "Renamed '%s' to '%s' to prevent re-processing.",
                jobs_file_path, processed_file_path
            )

            # Now that the file is renamed, we can safely push to Redis.
            r.lpush(key(self.campaign_name, 'queues:scan'), *initial_jobs)

            # Set initial campaign state if not already set.
            state_key = key(self.campaign_name, 'campaign:state')
            if not r.hget(state_key, 'status'):
                r.hset(state_key, 'status', 'running')

            logging.info(
                "Successfully primed Redis with %d scan jobs.", len(initial_jobs)
            )

        except Exception as e:
            logging.error("Failed to prime initial jobs: %s", e, exc_info=True)
            # A failure here might require manual intervention (e.g., reprime).

    def _recover_state_on_leadership(self, r: redis.Redis):
        """Cleans up state from a previous, potentially crashed, leader."""
        logging.info("New leader elected. Recovering stranded jobs and workers.")
        pipe = r.pipeline()
        scans_inprogress_key = key(self.campaign_name, 'scans:inprogress')
        stranded_scans = r.hvals(scans_inprogress_key)
        if stranded_scans:
            logging.warning("Found %d stranded scan jobs. Re-queueing.", len(stranded_scans))
            pipe.lpush(key(self.campaign_name, 'queues:scan'), *stranded_scans)
            pipe.delete(scans_inprogress_key)
            pipe.delete(key(self.campaign_name, 'scans:counts'))

        # Use HVALS on the new HASH to find stranded migration jobs
        migrations_inprogress_key = key(self.campaign_name, 'migrations:inprogress')
        stranded_migrations = r.hvals(migrations_inprogress_key)
        if stranded_migrations:
            logging.warning("Found %d stranded migration files. Re-queueing.", len(stranded_migrations))
            pipe.lpush(key(self.campaign_name, 'queues:migrate'), *stranded_migrations)
            pipe.delete(migrations_inprogress_key)

        logging.info("Cleaning up stale worker status and rate entries...")
        pipe.delete(key(self.campaign_name, 'status'))
        pipe.delete(key(self.campaign_name, 'rates:migrate'))
        pipe.delete(key(self.campaign_name, 'rates:scan'))
        pipe.delete(key(self.campaign_name, 'rates:bandwidth'))

        results = pipe.execute()
        if any(results):
            logging.info("Successfully recovered state.")
        else:
            logging.info("No state to recover (clean previous shutdown).")

    def _stop_redis_server(self):
        """Stops the leader's Redis server process gracefully."""
        if self.redis_proc and self.redis_proc.poll() is None:
            logging.info("Shutting down Redis server process...")
            self.redis_proc.terminate()
            try:
                self.redis_proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                logging.warning("Redis did not terminate gracefully, killing.")
                self.redis_proc.kill()
        self.redis_proc = None

    def _release_lock(self):
        """Releases the leader file lock."""
        if self.lock_file_handle:
            fcntl.flock(self.lock_file_handle, fcntl.LOCK_UN)
            self.lock_file_handle.close()
            self.lock_file_handle = None

        if os.path.exists(self.lock_file_path):
            try:
                os.remove(self.lock_file_path)
            except OSError as e:
                logging.warning("Could not remove lock file %s: %s", self.lock_file_path, e)
        self.is_leader = False
        logging.info("Leader lock released.")

    def run(self) -> bool:
        """
        The main entry point for the daemon process.

        Sets up leader/follower state, then supervises worker threads.
        Returns True for a clean shutdown, False if an error occurred.
        """
        r = None
        worker = None
        try:
            # Phase 1: Become Leader or connect as Follower
            while not self.shutdown_flag:
                self._try_acquire_lock()
                if self.is_leader:
                    self._start_redis_server()
                    r = self._wait_for_redis(host=self.advertise_address)
                    r.hset(key(self.campaign_name, 'campaign:state'), 'leader', self.hostname)
                    self._recover_state_on_leadership(r)
                    self._prime_initial_jobs(r)
                    break  # Exit loop, we are the leader

                logging.info("Running as follower. Attempting to find leader.")
                leader_info_path = os.path.join(self.campaign_path, LEADER_INFO_FILE)
                if not os.path.exists(leader_info_path):
                    logging.info("Leader info file not found. Waiting...")
                    time.sleep(5)
                    continue

                with open(leader_info_path, 'r', encoding='utf-8') as f:
                    host = f.read().strip().split(':')[0]
                try:
                    # Followers use get_redis_connection, which now handles passwords
                    r = get_redis_connection(self.campaign_name)
                    logging.info("Successfully connected to leader at %s.", host)
                    break  # Exit loop, we are a follower
                except (ConnectionError, InterruptedError):
                    logging.warning("Could not connect to leader at %s. Retrying...", host)
                    time.sleep(5)

            if self.shutdown_flag:
                return not self.shutdown_due_to_error

            # Phase 2: Initialize Worker and Start all Thread Pools
            config = load_config(self.campaign_name)
            worker = Worker(
                self.campaign_name, config, r, self.dry_run, self.hostname,
                self.max_pending_migrations
            )
            self.metrics_server = MetricsServer(worker, r, self.campaign_name, config)
            r.hset(key(self.campaign_name, 'status'), self.hostname, "idle")

            logging.info("Starting %d scan worker thread(s)...", self.num_scan_workers)
            for i in range(self.num_scan_workers):
                thread = threading.Thread(
                    target=self._scan_thread_target,
                    args=(worker, r),
                    name=f"ScanWorker-{i+1}",
                    daemon=True
                )
                self.worker_threads.append(thread)
                thread.start()

            logging.info("Starting %d migration worker thread(s)...", self.num_migration_workers)
            for i in range(self.num_migration_workers):
                thread = threading.Thread(
                    target=self._migrate_thread_target,
                    args=(worker, r),
                    name=f"MigrateWorker-{i+1}",
                    daemon=True
                )
                self.worker_threads.append(thread)
                thread.start()

            self.metrics_server.start()
            logging.info("All threads started. Main thread entering supervision loop.")

            # Phase 3: Supervision Loop
            while not self.shutdown_flag:
                if self.is_leader:
                    self._check_leader_tasks(r)
                time.sleep(10)  # Main loop sleep interval

        except Exception as e:
            if not self.shutdown_flag:
                logging.critical("Fatal error in main daemon: %s", e, exc_info=True)
                self.shutdown_due_to_error = True
        finally:
            self._cleanup(r, worker)
            return not self.shutdown_due_to_error

    def _check_leader_tasks(self, r: redis.Redis):
        """Periodic tasks performed only by the leader."""
        now = time.time()

        # Check for lost lock
        if now - self.last_lock_check > 10:
            self.last_lock_check = now
            try:
                fcntl.flock(self.lock_file_handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except (IOError, OSError):
                if not self.shutdown_flag:
                    logging.critical(
                        "Lost leader lock! Shutting down to allow another "
                        "node to take over."
                    )
                    self.shutdown_due_to_error = True
                    self.shutdown_flag = True

        # Check for campaign completion
        if now - self.last_completion_check > 30:
            self.last_completion_check = now
            pipe = r.pipeline()
            pipe.llen(key(self.campaign_name, 'queues:scan'))
            pipe.hlen(key(self.campaign_name, 'scans:inprogress'))
            pipe.llen(key(self.campaign_name, 'queues:migrate'))
            # Use HLEN on the new HASH to check for in-progress migrations
            pipe.hlen(key(self.campaign_name, 'migrations:inprogress'))
            queue_counts = pipe.execute()

            if sum(queue_counts) == 0:
                log_path = os.path.join(self.campaign_path, LOGS_DIR)
                logging.info(
                    "Campaign '%s' completed successfully. All queues are empty.",
                    self.campaign_name
                )
                logging.info("Success/failure logs are stored in: %s", log_path)
                logging.info("Instructing all workers to perform a clean shutdown.")
                r.hset(key(self.campaign_name, 'campaign:state'), 'status', 'completed')
                self.shutdown_flag = True

    def _cleanup(self, r: redis.Redis, worker: Worker):
        """Unified cleanup logic for shutdown."""
        logging.info("Daemon shutting down. Cleaning up all components...")
        if worker:
            worker.shutdown()
        logging.info("Waiting for %d worker thread(s) to terminate...", len(self.worker_threads))
        for thread in self.worker_threads:
            if thread.is_alive():
                thread.join(timeout=2)
        if r:
            try:
                # Followers should remove their status entry
                if not self.is_leader:
                    r.hdel(key(self.campaign_name, 'status'), self.hostname)
                r.close()
            except Exception as e:
                logging.warning("Error during Redis cleanup: %s", e)
        if self.is_leader:
            self._stop_redis_server()
            self._release_lock()
        logging.info("Cleanup complete. Daemon process will now exit.")
