import logging
from typing import Optional

import ydb

from .metrics import MetricsCollector

logger = logging.getLogger(__name__)


class BaseExecutor:
    """
    Base class for executing YDB operations with common patterns.

    Provides:
    - Branch ID range management (bid_from, bid_to)
    - Table folder management
    - Metrics collection (optional)
    - Two execution modes: pooled and single-session
    - Iteration pattern for executing operations multiple times
    """

    def __init__(
        self,
        bid_from: int,
        bid_to: int,
        count: int,
        metrics_collector: Optional[MetricsCollector] = None,
        table_folder: str = "pgbench",
        use_single_session: bool = False,
    ):
        """
        Initialize executor with branch ID range and operation count.

        Args:
            bid_from: Starting branch ID (inclusive)
            bid_to: Ending branch ID (inclusive)
            count: Number of operations to execute
            metrics_collector: Optional metrics collector for tracking performance
            table_folder: Folder name for tables (default: "pgbench")
            use_single_session: If True, use single session mode; if False, use pooled mode (default)
        """
        self._bid_from = bid_from
        self._bid_to = bid_to
        self._count = count
        self._metrics = metrics_collector
        self._table_folder = table_folder
        self._use_single_session = use_single_session

    async def execute(self, pool: ydb.aio.QuerySessionPool):
        """
        Execute operations using the configured execution mode.

        Args:
            pool: YDB query session pool
        """
        if self._use_single_session:
            await self._execute_single_session(pool)
        else:
            await self._execute_pooled(pool)

    async def _execute_pooled(self, pool: ydb.aio.QuerySessionPool):
        """
        Execute operations using pool's retry mechanism.

        Args:
            pool: YDB query session pool
        """
        logger.info(f"{self.__class__.__name__} [{self._bid_from}, {self._bid_to}] started")
        for i in range(self._count):
            await pool.retry_operation_async(lambda session: self._execute_operation(session, i))
        logger.info(f"{self.__class__.__name__} [{self._bid_from}, {self._bid_to}] completed")

    async def _execute_single_session(self, pool: ydb.aio.QuerySessionPool):
        """
        Execute operations using a single acquired session.

        Args:
            pool: YDB query session pool
        """
        logger.info(f"{self.__class__.__name__} [{self._bid_from}, {self._bid_to}] started")
        session = await pool.acquire()
        try:
            for i in range(self._count):
                await self._execute_operation(session, i)
        finally:
            await pool.release(session)
        logger.info(f"{self.__class__.__name__} [{self._bid_from}, {self._bid_to}] completed")

    async def _execute_operation(self, session: ydb.aio.QuerySession, iteration: int):
        """
        Abstract method to be implemented by subclasses.
        Executes a single operation (transaction, initialization, etc.)

        Args:
            session: YDB query session
            iteration: Current iteration number (0-based)
        """
        raise NotImplementedError("Subclasses must implement _execute_operation")
