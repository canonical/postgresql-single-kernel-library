# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""Background process for rotating logs."""

import logging
import os
import signal
import subprocess

from ops.model import ActiveStatus

from single_kernel_postgresql.config.literals import PGBACKREST_LOGROTATE_FILE
from single_kernel_postgresql.core.state import CharmState
from single_kernel_postgresql.managers.base import BaseManager
from single_kernel_postgresql.workload.base import BaseWorkload

logger = logging.getLogger(__name__)

# File path for the spawned rotate logs process to write logs.
LOG_FILE_PATH = "/var/log/rotate_logs.log"


class ObserverManager(BaseManager):
    """Manager for various observeer scripts."""

    def __init__(self, state: CharmState, workload: BaseWorkload) -> None:
        super().__init__(state, workload, "observer_manager")

    def start_log_rotation(self):
        """Start the rotate logs running in a new process."""
        if (
            not isinstance(self.state.peer.unit.status, ActiveStatus)
            or self.state.peer_relation is None
            or not os.path.exists(PGBACKREST_LOGROTATE_FILE)
        ):
            return
        if pid := self.state.peer.rotate_logs_pid:
            # Double check that the PID exists.
            try:
                os.kill(pid, 0)
                return
            except OSError:
                pass

        logging.info("Starting rotate logs process")

        pid = subprocess.Popen(
            ["/usr/bin/python3", "scripts/rotate_logs.py"],
            # File should not close
            stdout=open(LOG_FILE_PATH, "a"),  # noqa: SIM115
            stderr=subprocess.STDOUT,
        ).pid

        self.state.peer.rotate_logs_pid = pid
        logging.info(f"Started rotate logs process with PID {pid}")

    def stop_log_rotation(self) -> None:
        """Stop the running rotate logs process if we have previously started it."""
        if pid := self.state.peer.rotate_logs_pid:
            try:
                os.kill(pid, signal.SIGINT)
                logging.info(f"Stopped rotate logs process with PID {pid}")
                self.state.peer.rotate_logs_pid = None
            except OSError:
                pass
