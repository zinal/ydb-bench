import asyncio
import logging
import math
import os
import sys
from contextlib import asynccontextmanager
from typing import AsyncIterator, Optional, Sequence, Tuple

import ydb

from .base_executor import BaseExecutor
from .metrics import MetricsCollector
from .workload import WeightedScriptSelector

logger = logging.getLogger(__name__)


class Runner:
    def __init__(
        self,
        endpoint: str,
        database: str,
        root_certificates_file: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        table_folder: str = "pgbench",
        scale: int = 100,
    ):
        """
        Initialize YdbExecutor with YDB connection parameters.

        Args:
            endpoint: YDB endpoint (e.g., "grpcs://ydb-host:2135")
            database: Database path (e.g., "/Root/database")
            root_certificates_file: Optional path to root certificate file for TLS
            user: Optional username for authentication
            password: Optional password for authentication
            table_folder: Folder name for tables (default: "pgbench")
            scale: Number of branches (default: 100)
        """
        # Load root certificates from file if provided
        root_certificates = None
        if root_certificates_file:
            root_certificates = ydb.load_ydb_root_certificate(root_certificates_file)

        # Create driver configuration
        self._config = ydb.DriverConfig(endpoint=endpoint, database=database, root_certificates=root_certificates)

        # Create credentials from username and password if provided
        self._credentials = None
        if user and password:
            self._credentials = ydb.StaticCredentials(self._config, user=user, password=password)

        # Store table folder and scale for use in operations
        self.table_folder = table_folder
        self.scale = scale

    @asynccontextmanager
    async def _get_pool(self) -> AsyncIterator[ydb.aio.QuerySessionPool]:
        """
        Async context manager that creates and yields a YDB QuerySessionPool.
        Handles driver initialization, connection waiting, and cleanup.
        """
        async with ydb.aio.Driver(driver_config=self._config, credentials=self._credentials) as driver:
            await driver.wait()
            logger.info("Connected to YDB")
            await asyncio.sleep(3)  # Required to make node discovery work
            logger.info("Starting operations")
            async with ydb.aio.QuerySessionPool(driver) as pool:
                yield pool

    async def _run_executors_parallel(self, pool: ydb.aio.QuerySessionPool, executors: Sequence[BaseExecutor]) -> None:
        """
        Run multiple executors in parallel using asyncio.gather.

        Args:
            pool: YDB query session pool
            executors: List of BaseExecutor instances to run in parallel
        """
        await asyncio.gather(*[executor.execute(pool) for executor in executors])

    def init_tables(self, job_count: int = 10) -> None:
        """
        Initialize database tables with the specified scale factor.

        Args:
            job_count: Number of parallel jobs for filling tables (default: 10)
        """
        from .initializer import Initializer

        # Create initializer instances for parallel execution
        initializers = []
        for i in range(job_count):
            bid_from, bid_to = Runner._make_bid_range(self.scale, job_count, i)
            initializers.append(Initializer(bid_from, bid_to, table_folder=self.table_folder))

        async def _init() -> None:
            async with self._get_pool() as pool:
                # Create tables first (DDL operations)
                initer = Initializer(1, self.scale, table_folder=self.table_folder)
                await initer.create_tables(pool)

                # Fill tables in parallel
                await self._run_executors_parallel(pool, initializers)

        asyncio.run(_init())

    def run(
        self,
        process_id: int,
        job_count: int = 7,
        tran_count: int = 100,
        use_single_session: bool = False,
        script_selector: Optional[WeightedScriptSelector] = None,
    ) -> MetricsCollector:
        """
        Run workload with specified number of jobs and transactions.
        This method will be executed in a separate process (or single process).

        Args:
            process_id: Process identifier for logging
            job_count: Number of parallel jobs
            tran_count: Number of transactions per job
            use_single_session: If True, use single session mode; if False, use pooled mode (default)
            script_selector: Optional WeightedScriptSelector for multiple weighted scripts (if None, uses default script)

        Returns:
            MetricsCollector instance with collected metrics
        """
        from .job import Job

        pid = os.getpid()
        print(f"Process {process_id} started (PID: {pid})")

        metrics = MetricsCollector()

        try:
            # Create job instances for parallel execution
            jobs = []
            for i in range(job_count):
                bid_from, bid_to = Runner._make_bid_range(self.scale, job_count, i)
                jobs.append(
                    Job(
                        bid_from,
                        bid_to,
                        tran_count,
                        metrics,
                        self.table_folder,
                        use_single_session,
                        script_selector,
                    )
                )

            async def _run() -> None:
                mode = "single session" if use_single_session else "pooled"
                logger.info(f"Starting workload in {mode} mode")

                async with self._get_pool() as pool:
                    # Validate scale before starting jobs
                    await self._validate_scale(pool)

                    # Run jobs in parallel
                    await self._run_executors_parallel(pool, jobs)
                    logger.info("All jobs completed")

            asyncio.run(_run())
        except Exception as e:
            # Catch all exceptions and store in metrics
            error_msg = f"Process {process_id} (PID: {pid}) failed with error: {str(e)}"
            metrics.unhandled_error_messages.append(error_msg)
            
        return metrics

    async def _validate_scale(self, pool: ydb.aio.QuerySessionPool) -> None:
        """
        Validate that the requested scale doesn't exceed the number of branches in the database.

        Args:
            pool: YDB query session pool

        Raises:
            ValueError: If scale exceeds the number of branches in the database
        """
        result = await pool.execute_with_retries(
            f"SELECT COUNT(*) as branch_count FROM `{self.table_folder}/branches`;"
        )

        # Extract the count from result
        branch_count = 0
        for row in result[0].rows:
            branch_count = row["branch_count"]
            break

        if self.scale > branch_count:
            raise ValueError(
                f"Scale {self.scale} exceeds the number of initialized branches ({branch_count}). "
                f"Please run 'init' with scale >= {self.scale} or reduce the scale parameter."
            )

        logger.info(f"Scale validation passed: {self.scale} <= {branch_count} branches")

    @staticmethod
    def _make_bid_range(scale: int, job_count: int, job_index: int) -> Tuple[int, int]:
        return (
            math.floor(float(scale) / job_count * job_index) + 1,
            math.floor(float(scale) / job_count * (job_index + 1)),
        )
