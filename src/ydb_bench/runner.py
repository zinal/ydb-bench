import asyncio
import logging
import math
import os
import sys
from contextlib import asynccontextmanager
from typing import AsyncIterator, List, Optional, Sequence, Tuple

import ydb

from .base_executor import BaseExecutor
from .metrics import MetricsCollector
from .workload import WeightedScriptSelector

logger = logging.getLogger(__name__)


def split_range(start: int, end: int, count: int) -> List[Tuple[int, int]]:
    """
    Split range [start, end] into count non-overlapping sub-ranges.

    If count exceeds the number of elements in the range, returns count ranges
    where some ranges will be duplicates of single elements.

    Args:
        start: Start of the range (inclusive)
        end: End of the range (inclusive)
        count: Number of sub-ranges to create

    Returns:
        List of tuples representing (start, end) for each sub-range.
        All ranges are guaranteed to be non-empty (start <= end).

    Example:
        split_range(1, 100, 4) -> [(1, 25), (26, 50), (51, 75), (76, 100)]
        split_range(1, 2, 3) -> [(1, 1), (1, 1), (2, 2)]
        split_range(1, 5, 10) -> [(1, 1), (1, 1), (2, 2), (2, 2), (3, 3), (3, 3), (4, 4), (4, 4), (5, 5), (5, 5)]
    """
    if count <= 0:
        raise ValueError("count must be positive")
    if start > end:
        raise ValueError("start must be <= end")

    total_size = end - start + 1
    ranges = []

    # When count > total_size, create single-element ranges with duplicates
    if count > total_size:
        for i in range(count):
            # Map each range index to an element, allowing duplicates
            element_index = math.floor(float(i) * total_size / count)
            element_value = start + element_index
            ranges.append((element_value, element_value))
    else:
        # Normal case: split range into count parts
        for i in range(count):
            range_start = start + math.floor(float(total_size) / count * i)
            range_end = start + math.floor(float(total_size) / count * (i + 1)) - 1
            # Adjust last range to include any remainder
            if i == count - 1:
                range_end = end
            ranges.append((range_start, range_end))

    return ranges


class Runner:
    def __init__(
        self,
        endpoint: str,
        database: str,
        bid_from: int,
        bid_to: int,
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
            bid_from: Starting branch ID (inclusive)
            bid_to: Ending branch ID (inclusive)
            root_certificates_file: Optional path to root certificate file for TLS
            user: Optional username for authentication
            password: Optional password for authentication
            table_folder: Folder name for tables (default: "pgbench")
        """
        # Store connection parameters for creating copies and dynamic config creation
        self._endpoint = endpoint
        self._database = database
        self._root_certificates_file = root_certificates_file
        self._user = user
        self._password = password

        # Store table folder and bid range for use in operations
        self.table_folder = table_folder
        self.bid_from = bid_from
        self.bid_to = bid_to

    @asynccontextmanager
    async def _get_pool(self) -> AsyncIterator[ydb.aio.QuerySessionPool]:
        """
        Async context manager that creates and yields a YDB QuerySessionPool.
        Handles driver initialization, connection waiting, and cleanup.
        Creates config and credentials dynamically to avoid pickling issues.
        """
        # Load root certificates from file if provided
        root_certificates = None
        if self._root_certificates_file:
            root_certificates = ydb.load_ydb_root_certificate(self._root_certificates_file)

        # Create credentials from username and password if provided
        credentials = None
        if self._user:
            credentials = ydb.StaticCredentials.from_user_password(self._user, self._password)
        else:
            credentials = ydb.credentials_from_env_variables()
            print(credentials)

        # Create driver configuration
        config = ydb.DriverConfig(
            endpoint=self._endpoint,
            database=self._database,
            root_certificates=root_certificates,
            credentials=credentials,
            use_all_nodes=True,
        )

        async with ydb.aio.Driver(driver_config=config) as driver:
            await driver.wait()
            logger.info("Connected to YDB")
            await asyncio.sleep(3)  # Required to make node discovery work
            logger.info("Starting operations")
            async with ydb.aio.QuerySessionPool(driver) as pool:
                yield pool

    def split(self, n: int) -> List["Runner"]:
        """
        Split this runner into n non-overlapping copies with different bid ranges.

        Args:
            n: Number of copies to create

        Returns:
            List of Runner instances, each with a non-overlapping bid range
        """
        ranges = split_range(self.bid_from, self.bid_to, n)
        runners = []

        for bid_from, bid_to in ranges:
            runner = Runner(
                endpoint=self._endpoint,
                database=self._database,
                bid_from=bid_from,
                bid_to=bid_to,
                root_certificates_file=self._root_certificates_file,
                user=self._user,
                password=self._password,
                table_folder=self.table_folder,
            )
            runners.append(runner)

        return runners

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
        Initialize database tables with the specified bid range.

        Args:
            job_count: Number of parallel jobs for filling tables (default: 10)
        """
        from .initializer import Initializer

        # Create initializer instances for parallel execution
        initializers = []
        ranges = split_range(self.bid_from, self.bid_to, job_count)
        for bid_from, bid_to in ranges:
            initializers.append(Initializer(bid_from, bid_to, table_folder=self.table_folder))

        async def _init() -> None:
            async with self._get_pool() as pool:
                # Create tables first (DDL operations)
                initer = Initializer(self.bid_from, self.bid_to, table_folder=self.table_folder)
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
        preheat: int = 0,
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
            preheat: Number of preheat transactions to run before counting metrics (default: 0)

        Returns:
            MetricsCollector instance with collected metrics
        """
        from .job import Job

        pid = os.getpid()
        print(f"Process {process_id} started (PID: {pid})")

        metrics = MetricsCollector()

        try:
            # Create job instances for parallel execution
            # Total transaction count includes preheat transactions
            total_tran_count = tran_count + preheat
            jobs = []
            ranges = split_range(self.bid_from, self.bid_to, job_count)
            for job_bid_from, job_bid_to in ranges:
                jobs.append(
                    Job(
                        job_bid_from,
                        job_bid_to,
                        total_tran_count,
                        metrics,
                        self.table_folder,
                        use_single_session,
                        script_selector,
                        preheat,
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
        Validate that the requested scale is valid and within the initialized branches.

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

        if self.bid_to > branch_count:
            raise ValueError(
                f"Bid range [{self.bid_from}, {self.bid_to}] exceeds the number of initialized branches ({branch_count}). "
                f"Please run 'init' with scale >= {self.bid_to} or reduce the scale parameter."
            )

        logger.info(
            f"Scale validation passed: bid range [{self.bid_from}, {self.bid_to}] within {branch_count} branches"
        )
