import logging
import re
import time
from random import randint
from typing import Optional

import ydb

from .base_executor import BaseExecutor
from .constants import ACCOUNTS_PER_BRANCH, DEFAULT_SCRIPT, TELLERS_PER_BRANCH
from .metrics import MetricsCollector

logger = logging.getLogger(__name__)


class Job(BaseExecutor):
    """
    Executes pgbench-like workload transactions.

    Uses random branch selection within the range for each transaction.
    """

    def __init__(
        self,
        bid_from: int,
        bid_to: int,
        tran_count: int,
        metrics_collector: Optional[MetricsCollector] = None,
        table_folder: str = "pgbench",
        use_single_session: bool = False,
        script: Optional[str] = None,
    ):
        """
        Initialize a job that executes transactions.

        Args:
            bid_from: Starting branch ID (inclusive)
            bid_to: Ending branch ID (inclusive)
            tran_count: Number of transactions to execute
            metrics_collector: Optional metrics collector for tracking performance
            table_folder: Folder name for tables (default: "pgbench")
            use_single_session: If True, use single session mode; if False, use pooled mode
            script: SQL script to execute (default: DEFAULT_SCRIPT from constants)
        """
        super().__init__(
            bid_from,
            bid_to,
            tran_count,
            metrics_collector,
            table_folder,
            use_single_session,
        )

        # Use default script if none provided
        script_template = script if script is not None else DEFAULT_SCRIPT

        # Format script with table_folder in constructor
        self._script = script_template.format(table_folder=self._table_folder)

        # Detect which parameters are used in the script
        self._uses_bid = "$bid" in self._script
        self._uses_tid = "$tid" in self._script
        self._uses_aid = "$aid" in self._script
        self._uses_delta = "$delta" in self._script
        self._uses_iteration = "$iteration" in self._script

    def _build_parameters(self, iteration: int) -> dict:
        """
        Build parameters dictionary based on what's used in the script.
        Generates random values for bid, tid, aid, and delta.

        Args:
            iteration: Current iteration number

        Returns:
            Dictionary of parameters for the query
        """
        # Generate random values
        bid = randint(self._bid_from, self._bid_to)
        tid = (bid - 1) * TELLERS_PER_BRANCH + randint(1, TELLERS_PER_BRANCH)
        aid = (bid - 1) * ACCOUNTS_PER_BRANCH + randint(1, ACCOUNTS_PER_BRANCH)
        delta = randint(1, 1000)

        parameters = {}
        if self._uses_bid:
            parameters["$bid"] = ydb.TypedValue(bid, ydb.PrimitiveType.Int32)
        if self._uses_tid:
            parameters["$tid"] = ydb.TypedValue(tid, ydb.PrimitiveType.Int32)
        if self._uses_aid:
            parameters["$aid"] = ydb.TypedValue(aid, ydb.PrimitiveType.Int32)
        if self._uses_delta:
            parameters["$delta"] = ydb.TypedValue(delta, ydb.PrimitiveType.Int32)
        if self._uses_iteration:
            parameters["$iteration"] = ydb.TypedValue(iteration, ydb.PrimitiveType.Int32)
        return parameters

    async def _execute_operation(self, session: ydb.aio.QuerySession, iteration: int):
        """
        Execute a single pgbench-like transaction.

        Args:
            session: YDB query session
            iteration: Current iteration number (0-based)
        """
        start_time = time.time()
        success = False
        error_message = ""
        total_duration_us = 0
        total_cpu_time_us = 0

        try:
            # Build parameters dictionary with random values
            parameters = self._build_parameters(iteration)

            async with session.transaction() as tx:
                async with await tx.execute(
                    self._script,
                    parameters=parameters,
                    commit_tx=True,
                    stats_mode=ydb.QueryStatsMode.BASIC,
                ) as results:
                    async for result in results:
                        # All results should be obtained to get last_query_stats
                        pass
                    total_duration_us = tx.last_query_stats.total_duration_us
                    total_cpu_time_us = tx.last_query_stats.total_cpu_time_us
            success = True
        except Exception as e:
            error_message = str(e)

            raise
        finally:
            end_time = time.time()
            if self._metrics:
                self._metrics.record_transaction(
                    start_time,
                    end_time,
                    success,
                    error_message,
                    total_duration_us,
                    total_cpu_time_us,
                )
