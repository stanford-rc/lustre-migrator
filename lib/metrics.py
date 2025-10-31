# Copyright (C) 2025
#      The Board of Trustees of the Leland Stanford Junior University
# Written by Stephane Thiell <sthiell@stanford.edu>
#
# Licensed under GPL v3 (see https://www.gnu.org/licenses/).

import logging
from threading import Thread

from flask import Blueprint, Flask, current_app, jsonify

from .common import APP_CONFIG, key

# Use a Blueprint for better app organization
metrics_bp = Blueprint('metrics', __name__)


@metrics_bp.route('/metrics')
def get_metrics():
    """Gathers and returns local and global metrics for monitoring."""
    app = current_app

    # Context is passed to the Flask app from the app factory
    worker = app.worker
    r = app.r
    campaign_name = app.campaign_name
    config = app.config_dict

    # --- Local Metrics (specific to this worker process) ---
    status_key = key(campaign_name, 'status')
    worker_status_bytes = r.hget(status_key, worker.hostname)
    worker_status = "idle"
    if worker_status_bytes:
        worker_status = worker_status_bytes.decode('utf-8', 'ignore')

    local_metrics = {
        "hostname": worker.hostname,
        "status": worker_status,
        "counters": {
            "scans_completed": worker.scans_completed,
            "files_migrated_succeeded": worker.files_migrated_succeeded,
            "files_migrated_failed": worker.files_migrated_failed,
        },
        "rates": {
            "migrate_rate_files_per_sec": worker.get_migrate_rate_per_sec(),
            "found_files_rate_per_sec": worker.get_found_files_rate_per_sec(),
        }
    }

    # --- Global Metrics (campaign-wide from Redis leader) ---
    try:
        pipe = r.pipeline()
        pipe.hget(key(campaign_name, 'campaign:state'), 'status')
        pipe.hget(key(campaign_name, 'campaign:state'), 'leader')
        pipe.scard(key(campaign_name, 'sets:discovered'))
        pipe.scard(key(campaign_name, 'sets:succeeded'))
        pipe.scard(key(campaign_name, 'sets:failed'))
        pipe.scard(key(campaign_name, 'sets:inprogress'))
        pipe.llen(key(campaign_name, 'queues:scan'))
        pipe.llen(key(campaign_name, 'queues:migrate'))
        pipe.hlen(status_key)

        results = pipe.execute()

        campaign_status_raw = results[0]
        leader_raw = results[1]
        discovered = results[2]
        succeeded = results[3]
        failed = results[4]
        in_progress = results[5]
        scan_q = results[6]
        migrate_q = results[7]
        active_workers = results[8]

        campaign_status = 'running'
        if campaign_status_raw:
            campaign_status = campaign_status_raw.decode('utf-8', 'running')

        leader = 'unknown'
        if leader_raw:
            leader = leader_raw.decode('utf-8', 'unknown')

        global_metrics = {
            "campaign_name": config.get('campaign_name', 'unknown'),
            "target_osts": config.get('target_osts', ''),
            "campaign_status": campaign_status,
            "leader_hostname": leader,
            "active_workers": int(active_workers),
            "progress": {
                "files_discovered": int(discovered),
                "files_succeeded": int(succeeded),
                "files_failed": int(failed),
                "files_in_progress": int(in_progress),
            },
            "queues": {
                "scan_jobs_pending": int(scan_q),
                "migrate_jobs_pending": int(migrate_q),
            }
        }
    except Exception as e:
        logging.warning("Could not gather global metrics from Redis: %s", e)
        global_metrics = {}

    return jsonify({
        "global_metrics": global_metrics,
        "local_metrics": local_metrics,
    })


def create_app(worker_instance, redis_client, campaign, config_dict):
    """App factory to create and configure the Flask app."""
    app = Flask(__name__)
    app.register_blueprint(metrics_bp)

    # Attach necessary context to the app object for access in routes
    app.worker = worker_instance
    app.r = redis_client
    app.campaign_name = campaign
    app.config_dict = config_dict

    # Silence default Flask/Werkzeug messages for cleaner daemon logs
    log = logging.getLogger('werkzeug')
    log.setLevel(logging.WARNING)

    return app


class MetricsServer(Thread):
    """A thread that runs the Flask-based metrics server."""

    def __init__(self, worker_instance, redis_client, campaign, config_dict):
        super().__init__(daemon=True, name="MetricsServer")
        self.app = create_app(
            worker_instance, redis_client, campaign, config_dict
        )

    def run(self):
        """Starts the metrics server using the configured bind address."""
        bind_address = APP_CONFIG.METRICS_BIND_ADDRESS
        port = APP_CONFIG.METRICS_PORT

        logging.info(
            "Starting Flask metrics server on http://%s:%d/metrics",
            bind_address, port
        )

        try:
            # Host '0.0.0.0' makes it accessible from other machines
            self.app.run(host=bind_address, port=port, debug=False)
        except OSError as e:
            # Provide a specific, helpful error message for common errors
            if e.errno == 98:  # Address already in use
                logging.error(
                    "Metrics server failed: Port %d is already in use on "
                    "address %s.", port, bind_address
                )
            else:
                logging.error("Metrics server failed with an OS error: %s", e)
        except Exception as e:
            logging.error("Metrics server failed with an unexpected error: %s", e)
