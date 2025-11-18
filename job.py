import ydb
import logging
import time
from random import randint
from typing import Optional
from constants import TELLERS_PER_BRANCH, ACCOUNTS_PER_BRANCH
from metrics import MetricsCollector
from base_executor import BaseExecutor

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
        use_single_session: bool = False
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
        """
        super().__init__(bid_from, bid_to, tran_count, metrics_collector, table_folder, use_single_session)

    async def _execute_operation(self, session: ydb.aio.QuerySession, iteration: int):
        """
        Execute a single pgbench-like transaction.
        
        Args:
            session: YDB query session
            iteration: Current iteration number (0-based)
        """
        bid = randint(self._bid_from, self._bid_to)
        tid = (bid - 1) * TELLERS_PER_BRANCH + randint(1, TELLERS_PER_BRANCH)
        aid = (bid - 1) * ACCOUNTS_PER_BRANCH + randint(1, ACCOUNTS_PER_BRANCH)
        delta = randint(1, 1000)

        start_time = time.time()
        success = False
        error_message = ""
        total_duration_us = 0
        total_cpu_time_us = 0
        
        try:
            async with session.transaction() as tx:
                async with await tx.execute(
                    f"""
                        UPDATE `{self._table_folder}/accounts` SET abalance = abalance + $delta WHERE aid = $aid;
                        SELECT abalance FROM `{self._table_folder}/accounts` WHERE aid = $aid;
                        UPDATE `{self._table_folder}/tellers` SET tbalance = tbalance + $delta WHERE tid = $tid;
                        UPDATE `{self._table_folder}/branches` SET bbalance = bbalance + $delta WHERE bid = $bid;
                        INSERT INTO `{self._table_folder}/history` (tid, bid, aid, delta, mtime)
                        VALUES ($tid, $bid, $aid, $delta, CurrentUtcTimestamp());
                    """,
                    parameters={
                        "$tid": ydb.TypedValue(tid, ydb.PrimitiveType.Int32),
                        "$bid": ydb.TypedValue(bid, ydb.PrimitiveType.Int32),
                        "$aid": ydb.TypedValue(aid, ydb.PrimitiveType.Int32),
                        "$delta": ydb.TypedValue(delta, ydb.PrimitiveType.Int32),
                    },
                    commit_tx=True,
                    stats_mode=ydb.QueryStatsMode.BASIC,
                ) as results:
                    async for result in results:
                        # All resultsets should be obtained to get last_query_stats
                        pass
                    total_duration_us = tx.last_query_stats.total_duration_us
                    total_cpu_time_us = tx.last_query_stats.total_cpu_time_us
            success = True
        except Exception as e:
            error_message = str(e)
            logger.error(f"Transaction failed for bid={bid}: {e}", exc_info=True)
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
                    total_cpu_time_us
                )