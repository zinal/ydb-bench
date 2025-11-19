import asyncio
import logging
import math
from contextlib import asynccontextmanager
from typing import Optional

import ydb

from .metrics import MetricsCollector

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

        # Store table folder for use in operations
        self._table_folder = table_folder

    @asynccontextmanager
    async def _get_pool(self):
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

    async def _run_executors_parallel(self, pool: ydb.aio.QuerySessionPool, executors: list):
        """
        Run multiple executors in parallel using asyncio.gather.

        Args:
            pool: YDB query session pool
            executors: List of BaseExecutor instances to run in parallel
        """
        await asyncio.gather(*[executor.execute(pool) for executor in executors])

    def init_tables(self, scale: int = 100, job_count: int = 10):
        """
        Initialize database tables with the specified scale factor.

        Args:
            scale: Number of branches to create (defines range 1 to scale)
            job_count: Number of parallel jobs for filling tables (default: 10)
        """
        from .initializer import Initializer

        # Create initializer instances for parallel execution
        initializers = []
        for i in range(job_count):
            bid_from, bid_to = Runner._make_bid_range(scale, job_count, i)
            initializers.append(Initializer(bid_from, bid_to, table_folder=self._table_folder))

        async def _init():
            async with self._get_pool() as pool:
                # Create tables first (DDL operations)
                initer = Initializer(1, scale, table_folder=self._table_folder)
                await initer.create_tables(pool)

                # Fill tables in parallel
                await self._run_executors_parallel(pool, initializers)

        asyncio.run(_init())

    def run(
        self,
        job_count: int = 7,
        tran_count: int = 100,
        scale: int = 100,
        use_single_session: bool = False,
        script: Optional[str] = None,
    ):
        """
        Run workload with specified number of jobs and transactions.

        Args:
            job_count: Number of parallel jobs
            tran_count: Number of transactions per job
            scale: Number of branches (must not exceed initialized branches)
            use_single_session: If True, use single session mode; if False, use pooled mode (default)
            script: Optional SQL script to execute (if None, uses default script)
        """
        from .job import Job

        metrics = MetricsCollector()

        # Create job instances for parallel execution
        jobs = []
        for i in range(job_count):
            bid_from, bid_to = Runner._make_bid_range(scale, job_count, i)
            jobs.append(
                Job(
                    bid_from,
                    bid_to,
                    tran_count,
                    metrics,
                    self._table_folder,
                    use_single_session,
                    script,
                )
            )

        async def _run():
            mode = "single session" if use_single_session else "pooled"
            logger.info(f"Starting workload in {mode} mode")

            async with self._get_pool() as pool:
                # Validate scale before starting jobs
                await self._validate_scale(pool, scale)

                # Run jobs in parallel
                await self._run_executors_parallel(pool, jobs)
                logger.info("All jobs completed")

        asyncio.run(_run())

        # Return metrics without printing (caller will handle printing)
        return metrics

    async def _validate_scale(self, pool: ydb.aio.QuerySessionPool, scale: int):
        """
        Validate that the requested scale doesn't exceed the number of branches in the database.

        Args:
            pool: YDB query session pool
            scale: Requested scale value

        Raises:
            ValueError: If scale exceeds the number of branches in the database
        """
        result = await pool.execute_with_retries(
            f"SELECT COUNT(*) as branch_count FROM `{self._table_folder}/branches`;"
        )

        # Extract the count from result
        branch_count = 0
        for row in result[0].rows:
            branch_count = row["branch_count"]
            break

        if scale > branch_count:
            raise ValueError(
                f"Scale {scale} exceeds the number of initialized branches ({branch_count}). "
                f"Please run 'init' with scale >= {scale} or reduce the scale parameter."
            )

        logger.info(f"Scale validation passed: {scale} <= {branch_count} branches")

    @staticmethod
    def _make_bid_range(scale: int, job_count: int, job_index: int):
        return (
            math.floor(float(scale) / job_count * job_index) + 1,
            math.floor(float(scale) / job_count * (job_index + 1)),
        )
