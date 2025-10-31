import logging
import os
import signal
import subprocess
import threading
import time
from typing import Any, Dict, List, TextIO

import redis
from ClusterShell.RangeSet import RangeSet

from .common import APP_CONFIG, LOGS_DIR, get_campaign_path, key

# --- Rate Calculation Tuning ---
# The time window in seconds over which to average migration and scan rates.
RATE_AVERAGING_WINDOW_SECONDS = 300


def _stream_reader(stream, output_list: List[bytes]):
    """Reads from a stream line-by-line and appends to a list."""
    try:
        with stream:
            for line in iter(stream.readline, b''):
                output_list.append(line)
    except Exception as e:
        # This can happen during a forceful shutdown; log as debug.
        logging.debug("Stream reader error (expected during shutdown): %s", e)


class Worker:
    """
    Performs the actual work of scanning for files and migrating them.
    Instantiated once per daemon and shared by all worker threads.
    """
    def __init__(self, campaign_name: str, config: Dict[str, Any], r: redis.Redis, dry_run: bool, hostname: str, max_pending_migrations: int):
        self.campaign_name = campaign_name
        self.config = config
        self.r = r
        self.dry_run = dry_run
        self.hostname = hostname
        self.max_pending_migrations = max_pending_migrations
        self.lock = threading.Lock()
        self.scans_completed = 0
        self.files_migrated_succeeded = 0
        self.files_migrated_failed = 0
        self.migrate_rate_tracker: List[float] = []
        self.found_files_rate_tracker: List[tuple] = []
        self.bandwidth_rate_tracker: List[tuple] = []
        self.last_rate_report_time = time.time()
        self.campaign_path = get_campaign_path(self.campaign_name)
        self.succeeded_log_file: TextIO = None
        self.failed_log_file: TextIO = None
        self._open_log_files()

        try:
            ost_range_str = self.config['target_osts']
            self.expanded_osts = ','.join(RangeSet(ost_range_str))
            logging.info("Worker initialized for OST range: %s", ost_range_str)
        except Exception as e:
            ost_str = self.config.get('target_osts', 'MISSING')
            logging.critical(
                "FATAL: Invalid OST range in campaign config: '%s'", ost_str
            )
            raise ValueError(f"Invalid OST range configuration: {e}") from e

    def _open_log_files(self):
        """Opens log files for recording succeeded and failed migrations."""
        log_dir = os.path.join(self.campaign_path, LOGS_DIR)
        os.makedirs(log_dir, exist_ok=True)
        succeeded_path = os.path.join(log_dir, f"succeeded-{self.hostname}.log")
        failed_path = os.path.join(log_dir, f"failed-{self.hostname}.log")
        try:
            # Use a larger buffer (e.g., 4KB) instead of line buffering (1).
            # At high migration rates, line buffering can cause excessive `write()`
            # syscalls, stressing the shared filesystem. A larger buffer batches
            # many log lines into fewer, larger writes, improving performance.
            # The trade-off is a slight delay in log visibility and potential loss
            # of the last few seconds of logs in a hard crash scenario.
            buffer_size = 4096
            self.succeeded_log_file = open(
                succeeded_path, 'a', buffering=buffer_size, encoding='utf-8'
            )
            self.failed_log_file = open(
                failed_path, 'a', buffering=buffer_size, encoding='utf-8'
            )
            logging.info("Writing successful migrations to: %s", succeeded_path)
            logging.info("Writing failed migrations to: %s", failed_path)
        except OSError as e:
            logging.error("FATAL: Failed to open compliance log files: %s", e)
            self.succeeded_log_file = None
            self.failed_log_file = None

    def shutdown(self):
        """Closes any open resources, like log files."""
        logging.info("Closing worker log files.")
        if self.succeeded_log_file:
            self.succeeded_log_file.close()
        if self.failed_log_file:
            self.failed_log_file.close()

    def _update_migrate_rate(self):
        with self.lock:
            self.migrate_rate_tracker.append(time.time())

    def _update_bandwidth_rate(self, size: int):
        """Records the timestamp and size of a successful migration."""
        with self.lock:
            self.bandwidth_rate_tracker.append((time.time(), size))

    def _update_found_files_rate(self, count: int):
        with self.lock:
            self.found_files_rate_tracker.append((time.time(), count))

    def get_migrate_rate_per_sec(self) -> float:
        """Calculates migration rate over the configured time window."""
        with self.lock:
            window_start_time = time.time() - RATE_AVERAGING_WINDOW_SECONDS
            self.migrate_rate_tracker = [
                t for t in self.migrate_rate_tracker if t > window_start_time
            ]
            return len(self.migrate_rate_tracker) / float(RATE_AVERAGING_WINDOW_SECONDS)

    def get_bandwidth_rate_per_sec(self) -> float:
        """Calculates migration bandwidth (bytes/sec) over the configured time window."""
        with self.lock:
            window_start_time = time.time() - RATE_AVERAGING_WINDOW_SECONDS
            self.bandwidth_rate_tracker = [
                (t, s) for t, s in self.bandwidth_rate_tracker if t > window_start_time
            ]
            total_bytes = sum(s for t, s in self.bandwidth_rate_tracker)
            return total_bytes / float(RATE_AVERAGING_WINDOW_SECONDS)

    def get_found_files_rate_per_sec(self) -> float:
        """Calculates file discovery rate over the configured time window."""
        with self.lock:
            window_start_time = time.time() - RATE_AVERAGING_WINDOW_SECONDS
            self.found_files_rate_tracker = [
                (t, c) for t, c in self.found_files_rate_tracker
                if t > window_start_time
            ]
            total_files = sum(c for t, c in self.found_files_rate_tracker)
            return total_files / float(RATE_AVERAGING_WINDOW_SECONDS)

    def _report_rates(self, force: bool = False):
        """
        Reports the current rates for this worker to Redis.

        Args:
            force: If True, bypass the time-based throttle and report immediately.
                   This is crucial for reporting the final rate of a completed job.
        """
        now = time.time()
        if not force and (now - self.last_rate_report_time < 10):
            return

        try:
            # The get_*_rate_per_sec() methods below inherently trim the
            # rate tracker lists to a the configured window. Since _report_rates()
            # is called frequently, this prevents the lists from growing unboundedly.
            pipe = self.r.pipeline()
            pipe.hset(
                key(self.campaign_name, 'rates:migrate'),
                self.hostname,
                self.get_migrate_rate_per_sec()
            )
            pipe.hset(
                key(self.campaign_name, 'rates:bandwidth'),
                self.hostname,
                self.get_bandwidth_rate_per_sec()
            )
            pipe.hset(
                key(self.campaign_name, 'rates:scan'),
                self.hostname,
                self.get_found_files_rate_per_sec()
            )
            pipe.execute()
            self.last_rate_report_time = now
        except redis.exceptions.RedisError as e:
            logging.warning("Could not report rates to Redis: %s", e)

    def run_scan_job(self) -> bool:
        """Pops a path from the scan queue and processes it with 'lfs find'."""
        result = self.r.blpop(key(self.campaign_name, 'queues:scan'), timeout=5)
        if result is None:
            self._report_rates()  # Report rates even when idle
            return False

        worker_thread_id = f"{self.hostname}:{threading.current_thread().name}"
        scan_path_bytes = result[1]
        scan_path = scan_path_bytes.decode('utf-8')
        scans_inprogress_key = key(self.campaign_name, 'scans:inprogress')
        scans_counts_key = key(self.campaign_name, 'scans:counts')
        batch_size = APP_CONFIG.SCAN_BATCH_SIZE

        self.r.hset(scans_inprogress_key, worker_thread_id, scan_path)
        self.r.hset(scans_counts_key, worker_thread_id, 0)

        proc = None
        stderr_thread = None
        try:
            # Check if the scan path should be excluded
            for exclude in self.config.get('exclude_paths', []):
                if os.path.normpath(scan_path).startswith(os.path.normpath(exclude)):
                    logging.info("Skipping excluded path: %s (matches %s)", scan_path, exclude)
                    with self.lock:
                        self.scans_completed += 1
                    return True

            logging.info("Starting file discovery in: %s", scan_path)
            cmd = ["lfs", "find", "-O", self.expanded_osts, "--print0", scan_path]
            files_found_count = 0

            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=False)
            stderr_output: List[bytes] = []
            stderr_thread = threading.Thread(
                target=_stream_reader, args=(proc.stderr, stderr_output), daemon=True
            )
            stderr_thread.start()

            buffer = bytearray()
            batch = []

            # Process stdout stream for null-terminated file paths
            with proc.stdout:
                while True:
                    chunk = proc.stdout.read(4096)
                    if not chunk and proc.poll() is not None:
                        break
                    if not chunk:
                        continue

                    buffer.extend(chunk)
                    while b'\0' in buffer:
                        filename, _, remainder = buffer.partition(b'\0')
                        if filename:
                            batch.append(bytes(filename))
                            files_found_count += 1
                        buffer = remainder

                        if len(batch) >= batch_size:
                            # --- THROTTLING LOGIC WITH STATEFUL LOGGING ---
                            # This is where backpressure is triggered.
                            if self.max_pending_migrations > 0:
                                is_throttling = False
                                pending_count = self.r.llen(key(self.campaign_name, 'queues:migrate'))
                                while pending_count >= self.max_pending_migrations:
                                    if not is_throttling:
                                        logging.info(
                                            "[%s] Migration queue full (%d >= %d). Pausing discovery for '%s' to allow queue to drain.",
                                            worker_thread_id, pending_count, self.max_pending_migrations, scan_path
                                        )
                                        is_throttling = True
                                    # Set a flag to indicate throttling is active. The TTL ensures it auto-clears.
                                    self.r.set(key(self.campaign_name, 'scan:throttling_active'), 1, ex=20)
                                    # This sleep pauses our consumption of the pipe, causing `lfs find` to block.
                                    time.sleep(15)
                                    pending_count = self.r.llen(key(self.campaign_name, 'queues:migrate'))

                                if is_throttling:
                                     logging.info("[%s] Migration queue has drained. Resuming discovery for '%s'.", worker_thread_id, scan_path)

                            # Now push the batch to Redis.
                            pipe = self.r.pipeline()
                            pipe.sadd(key(self.campaign_name, 'sets:discovered'), *batch)
                            pipe.rpush(key(self.campaign_name, 'queues:migrate'), *batch)
                            pipe.hset(scans_counts_key, worker_thread_id, files_found_count)
                            pipe.execute()
                            self._update_found_files_rate(len(batch))
                            batch.clear()

                # The 'lfs find --print0' command should always end its output
                # with a null terminator. If any data remains in the buffer,
                # it signifies a corrupted stream. We log this as an error
                # and discard the data to prevent processing a partial/bad path.
                if buffer:
                    decoded_buffer = buffer[:100].decode('utf-8', 'replace')
                    logging.error(
                        "Scan of %s ended with %d bytes of non-null-terminated "
                        "data (discarded): %r",
                        scan_path, len(buffer), decoded_buffer
                    )
                # Push any final, valid batch of files.
                if batch:
                    pipe = self.r.pipeline()
                    pipe.sadd(key(self.campaign_name, 'sets:discovered'), *batch)
                    pipe.rpush(key(self.campaign_name, 'queues:migrate'), *batch)
                    pipe.execute()
                    self._update_found_files_rate(len(batch))

            return_code = proc.wait()
            stderr_thread.join()

            if return_code == 0:
                logging.info(
                    "Discovery for %s complete. Found %d files.",
                    scan_path, files_found_count
                )
                with self.lock:
                    self.scans_completed += 1
                return True

            # Handle failed scan
            if return_code == -signal.SIGTERM:
                logging.info(
                    "Discovery for %s interrupted by shutdown. Re-queueing.", scan_path
                )
            else:
                full_stderr = b''.join(stderr_output).decode('utf-8', 'ignore').strip()
                logging.error(
                    "Discovery for %s failed (RC=%d). Re-queueing. Stderr: %s",
                    scan_path, return_code, full_stderr
                )
            # Re-queue the job since it failed
            self.r.rpush(key(self.campaign_name, 'queues:scan'), scan_path_bytes)
            return False

        except Exception as e:
            logging.error(
                "Unexpected error during discovery of %s: %s", scan_path, e, exc_info=True
            )
            try:
                # Attempt to re-queue the job on error
                self.r.rpush(key(self.campaign_name, 'queues:scan'), scan_path_bytes)
            except redis.exceptions.RedisError as redis_e:
                logging.critical(
                    "Could not re-queue job for %s due to Redis error: %s",
                    scan_path, redis_e
                )
            return False
        finally:
            if proc and proc.poll() is None:
                proc.kill()
                proc.wait()
            if stderr_thread and stderr_thread.is_alive():
                stderr_thread.join()

            # This cleanup is now in the `finally` block. It unconditionally
            # removes this worker's entry from the in-progress tracking hashes.
            # This prevents "stranded" jobs if the worker dies unexpectedly.
            # The logic in the try/except blocks above is still responsible
            # for re-queueing the job if it fails.
            try:
                pipe = self.r.pipeline()
                pipe.hdel(scans_inprogress_key, worker_thread_id)
                pipe.hdel(scans_counts_key, worker_thread_id)
                pipe.execute()
            except redis.exceptions.RedisError as e:
                logging.warning("Failed to perform final cleanup for scan job: %s", e)

            # Force report rates after every job attempt to ensure the final
            # rate of a completed scan is not missed due to throttling.
            self._report_rates(force=True)

    def run_migrate_job(self) -> bool:
        """Pops a file path from the migrate queue and processes it."""
        result = self.r.blpop(key(self.campaign_name, 'queues:migrate'), timeout=5)
        if not result:
            self._report_rates()  # Report rates even when idle
            return False

        worker_thread_id = f"{self.hostname}:{threading.current_thread().name}"
        file_path_bytes = result[1]
        file_path_str = file_path_bytes.decode('utf-8', 'ignore')

        try:
            file_size = os.stat(file_path_str).st_size
        except FileNotFoundError:
            logging.warning("File not found before migration, skipping: %s", file_path_str)
            # This isn't a failure of the tool, the file is just gone.
            # We remove it from the 'discovered' set to keep counts accurate.
            self.r.srem(key(self.campaign_name, 'sets:discovered'), file_path_bytes)
            return True
        except OSError as e:
            logging.error("Could not stat file %s, marking as failed: %s", file_path_str, e)
            # Treat this as a migration failure.
            pipe = self.r.pipeline()
            pipe.sadd(key(self.campaign_name, 'sets:failed'), file_path_bytes)
            pipe.hset(key(self.campaign_name, 'errors'), file_path_bytes, str(e))
            pipe.execute()
            return True

        migrations_inprogress_key = key(self.campaign_name, 'migrations:inprogress')

        # The Redis operations below are not individually wrapped in try/except
        # blocks. This is a deliberate design choice. A Redis error during a
        # pipeline execution is typically a sign of a lost connection, which
        # is handled by the high-level `except ConnectionError` block in the
        # main thread loop (`_migrate_thread_target`). This triggers a full
        # daemon restart, which is our designated recovery mechanism.

        # Set status and track the specific file this worker is migrating
        pipe = self.r.pipeline()
        pipe.hset(key(self.campaign_name, 'status'), self.hostname, "migrating")
        pipe.hset(migrations_inprogress_key, worker_thread_id, file_path_bytes)
        pipe.execute()

        rc = 0
        stderr_msg = ""
        try:
            if self.dry_run:
                logging.debug("[DRY-RUN] Would migrate file: %s", file_path_str)
                time.sleep(0.01)
            else:
                logging.debug("Migrating file: %s", file_path_str)
                base_cmd = APP_CONFIG.LFS_MIGRATE_COMMAND[:]
                cmd = base_cmd
                flags = self.config.get("migration_flags")
                if flags:
                    cmd.extend(flags.split())
                cmd.append(file_path_bytes)

                try:
                    proc = subprocess.run(cmd, capture_output=True, check=True, text=False)
                    rc = proc.returncode
                except subprocess.CalledProcessError as e:
                    rc = e.returncode
                    stderr_msg = e.stderr.decode('utf-8', 'ignore').strip()
                    logging.warning(
                        "Failed to migrate %s. RC=%d. Stderr: %s",
                        file_path_str, rc, stderr_msg
                    )
                except Exception as e:
                    rc = -1
                    stderr_msg = str(e)
                    logging.error(
                        "Unexpected error migrating %s: %s", file_path_str, e
                    )

            # Update Redis state based on the outcome
            pipe = self.r.pipeline()
            if rc == 0:
                self.files_migrated_succeeded += 1
                self._update_migrate_rate()
                pipe.sadd(key(self.campaign_name, 'sets:succeeded'), file_path_bytes)
                self._update_bandwidth_rate(file_size)
                pipe.incrby(key(self.campaign_name, 'metrics:bytes_succeeded'), file_size)
                if self.succeeded_log_file:
                    self.succeeded_log_file.write(f"{repr(file_path_str)}\n")
            else:
                self.files_migrated_failed += 1
                pipe.sadd(key(self.campaign_name, 'sets:failed'), file_path_bytes)
                pipe.hset(key(self.campaign_name, 'errors'), file_path_bytes, stderr_msg)
                pipe.incrby(key(self.campaign_name, 'metrics:bytes_failed'), file_size)
                if self.failed_log_file:
                    clean_stderr = stderr_msg.replace(os.linesep, ' ')
                    log_line = f"{repr(file_path_str)} # RC={rc} # {clean_stderr}\n"
                    self.failed_log_file.write(log_line)
            pipe.execute()

        finally:
            # Always clean up the in-progress hash and worker status
            pipe = self.r.pipeline()
            pipe.hdel(migrations_inprogress_key, worker_thread_id)
            pipe.hset(key(self.campaign_name, 'status'), self.hostname, "idle")
            pipe.execute()

            self._report_rates()

        return True
