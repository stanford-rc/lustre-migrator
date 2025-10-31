# Copyright (C) 2025
#      The Board of Trustees of the Leland Stanford Junior University
# Written by Stephane Thiell <sthiell@stanford.edu>
#
# Licensed under GPL v3 (see https://www.gnu.org/licenses/).

import configparser
import ipaddress
import json
import logging
import os
import socket
from typing import Any, Dict

import redis

try:
    import netifaces
except ImportError:
    logging.critical(
        "The 'netifaces' library is required. Please run 'pip install netifaces'."
    )
    raise

# --- Global Config Object ---

class AppConfig:
    """Singleton-like object to hold app config from lustre-migrator.conf."""
    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config_path = None
        self._load_config()

    def _load_config(self):
        """Loads the main lustre-migrator.conf file from standard locations."""
        search_paths = [
            './lustre-migrator.conf',
            os.path.expanduser('~/.lustre-migrator.conf'),
            '/etc/lustre-migrator.conf'
        ]
        for path in search_paths:
            if os.path.exists(path):
                self.config.read(path)
                self.config_path = path
                return

    @property
    def BASE_PATH(self):
        return self.config.get('main', 'base_path', fallback='/var/lib/lustre-migrator')

    @property
    def SCAN_BATCH_SIZE(self):
        """Number of files to batch during a scan before queueing for migration."""
        return self.config.getint('main', 'scan_batch_size', fallback=1000)

    @property
    def REDIS_PORT(self):
        return self.config.getint('main', 'redis_port', fallback=6379)

    @property
    def METRICS_PORT(self):
        return self.config.getint('main', 'metrics_port', fallback=9188)

    @property
    def METRICS_BIND_ADDRESS(self):
        """The address for the metrics server to bind to."""
        return self.config.get(
            'main', 'metrics_bind_address', fallback='127.0.0.1'
        )

    @property
    def LFS_MIGRATE_COMMAND(self) -> list:
        """Gets the base lfs migrate command from config and splits it."""
        default_cmd = "lfs migrate -D -v"
        cmd_str = self.config.get(
            'main', 'lfs_migrate_command', fallback=default_cmd
        )
        return cmd_str.split()

    @property
    def COMMUNICATION_SUBNET(self):
        return self.config.get('main', 'communication_subnet', fallback=None)

# Instantiate the global config object
APP_CONFIG = AppConfig()


# --- Network and Hostname Helpers ---

def get_short_hostname() -> str:
    """
    Returns the short hostname of the machine.

    This is the single source of truth for the worker's identifier.
    e.g., 'my-node' from 'my-node.domain.com'
    """
    return socket.gethostname().split('.')[0]


def get_primary_ip() -> str:
    """
    Determines the primary IP by matching local interfaces against the configured
    communication_subnet. This is vital for multi-homed nodes.
    """
    subnet_str = APP_CONFIG.COMMUNICATION_SUBNET
    if not subnet_str:
        hostname = socket.getfqdn()
        logging.warning(
            "Config 'communication_subnet' is not set. Defaulting to FQDN "
            "'%s'. This can be unreliable and should be configured.",
            hostname
        )
        return hostname

    try:
        network = ipaddress.ip_network(subnet_str)
    except ValueError as e:
        raise ValueError(
            f"Invalid 'communication_subnet' in configuration: {e}"
        ) from e

    # Iterate through all network interfaces on the system
    for iface in netifaces.interfaces():
        ifaddrs = netifaces.ifaddresses(iface)
        # We only care about IPv4 addresses for this
        if netifaces.AF_INET in ifaddrs:
            # An interface can have multiple IPv4 addresses
            for addr_info in ifaddrs[netifaces.AF_INET]:
                ip_str = addr_info.get('addr')
                if not ip_str:
                    continue
                try:
                    ip_addr = ipaddress.ip_address(ip_str)
                    # Check if IP is on the configured subnet and not a loopback
                    if not ip_addr.is_loopback and ip_addr in network:
                        logging.info(
                            "Found matching IP %s on interface %s for subnet %s.",
                            ip_addr, iface, network
                        )
                        return str(ip_addr)
                except ValueError:
                    # Ignore invalid IP strings
                    continue

    raise ConnectionError(
        "Could not find any local network interface with an IP on the "
        f"configured subnet '{subnet_str}'"
    )


# --- Constants and Key Schema ---

CONFIG_FILE = "config.json"
REDIS_LOCK_FILE = "redis.lock"
LEADER_INFO_FILE = "leader.info"
REDIS_DATA_DIR = "redis-data"
INITIAL_JOBS_FILE = "initial_scan_jobs.list"
LOGS_DIR = "migration-logs"


def key(campaign: str, k: str) -> str:
    """Generates a standard Redis key for the campaign."""
    return f"lustre-migrator:{campaign}:{k}"


# --- Campaign and Connection Management ---

def get_campaign_path(campaign_name: str) -> str:
    """Gets the absolute path for a given campaign's data."""
    return os.path.join(APP_CONFIG.BASE_PATH, campaign_name)


def load_config(campaign_name: str) -> Dict[str, Any]:
    """Loads the campaign's config.json file."""
    config_path = os.path.join(get_campaign_path(campaign_name), CONFIG_FILE)
    if not os.path.exists(config_path):
        raise FileNotFoundError(
            f"Config file not found for campaign '{campaign_name}' at {config_path}"
        )
    with open(config_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def get_redis_connection(campaign_name: str) -> redis.Redis:
    """Connects to the leader's Redis instance via the leader info file."""
    leader_info_path = os.path.join(
        get_campaign_path(campaign_name), LEADER_INFO_FILE
    )
    if not os.path.exists(leader_info_path):
        raise ConnectionError("Leader info file not found. Is a leader running?")

    with open(leader_info_path, 'r', encoding='utf-8') as f:
        leader_host = f.read().strip().split(':')[0]

    try:
        # Pass the campaign_name as the password to authenticate with the
        # leader's Redis instance.
        client = redis.Redis(
            host=leader_host,
            port=APP_CONFIG.REDIS_PORT,
            db=0,
            password=campaign_name,
            decode_responses=False
        )
        client.ping()
        return client
    except redis.exceptions.AuthenticationError as e:
        raise ConnectionError(
            f"Redis authentication failed for campaign '{campaign_name}'. "
            f"Ensure all workers are on the same version. {e}"
        ) from e
    except redis.exceptions.ConnectionError as e:
        raise ConnectionError(
            f"Could not connect to Redis leader at "
            f"{leader_host}:{APP_CONFIG.REDIS_PORT}. {e}"
        ) from e


def setup_logging(name: str, level_str: str = 'INFO'):
    """Configures logging to standard error for systemd journal compatibility."""
    level = logging.getLevelName(level_str.upper())
    logging.basicConfig(
        level=level,
        format=f'{name}[%(process)d]: %(levelname)s %(message)s'
    )
