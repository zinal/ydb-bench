import ydb
import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Optional
import math

logger = logging.getLogger(__name__)


class Runner:
    def __init__(
        self,
        endpoint: str,
        database: str,
        root_certificates_file: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None
    ):
        """
        Initialize YdbExecutor with YDB connection parameters.
        
        Args:
            endpoint: YDB endpoint (e.g., "grpcs://ydb-host:2135")
            database: Database path (e.g., "/Root/database")
            root_certificates_file: Optional path to root certificate file for TLS
            user: Optional username for authentication
            password: Optional password for authentication
        """
        # Load root certificates from file if provided
        root_certificates = None
        if root_certificates_file:
            root_certificates = ydb.load_ydb_root_certificate(root_certificates_file)
        
        # Create driver configuration
        self._config = ydb.DriverConfig(
            endpoint=endpoint,
            database=database,
            root_certificates=root_certificates
        )
        
        # Create credentials from username and password if provided
        self._credentials = None
        if user and password:
            self._credentials = ydb.StaticCredentials(self._config, user=user, password=password)

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

    def init_tables(self, scale: int = 100):
        """Initialize database tables with the specified scale factor."""
        from initializer import Initializer
        
        # Store scale for later use in run()
        self._scale = scale
        
        async def _init():
            async with self._get_pool() as pool:
                initer = Initializer(scale)
                await initer.create_tables(pool)
                
                # Fill tables in parallel using TaskGroup
                async with asyncio.TaskGroup() as tg:
                    for i in range(scale):
                        tg.create_task(initer.fill_branch(pool, i + 1))
        
        asyncio.run(_init())

    def run(self, worker_count: int = 7, tran_count: int = 100, scale: int = 100, use_single_session: bool = False):
        """
        Run workload with specified number of workers and transactions.
        
        Args:
            worker_count: Number of parallel workers
            tran_count: Number of transactions per worker
            scale: Number of branches (must not exceed initialized branches)
            use_single_session: If True, use single session mode; if False, use pooled mode (default)
        """
        asyncio.run(self._execute_parallel(worker_count, tran_count, scale, use_single_session))
    
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
            "SELECT COUNT(*) as branch_count FROM `pgbench/branches`;"
        )
        
        # Extract the count from result
        branch_count = 0
        for row in result[0].rows:
            branch_count = row['branch_count']
            break
        
        if scale > branch_count:
            raise ValueError(
                f"Scale {scale} exceeds the number of initialized branches ({branch_count}). "
                f"Please run 'init' with scale >= {scale} or reduce the scale parameter."
            )
        
        logger.info(f"Scale validation passed: {scale} <= {branch_count} branches")

    async def _execute_parallel(self, worker_count: int, tran_count: int, scale: int, use_single_session: bool):
        """
        Execute workload in parallel with multiple workers.
        
        Args:
            worker_count: Number of parallel workers
            tran_count: Number of transactions per worker
            scale: Number of branches to distribute across workers
            use_single_session: If True, use single session mode; if False, use pooled mode
        """
        from worker import Worker
        
        mode = "single session" if use_single_session else "pooled"
        logger.info(f"Starting workload in {mode} mode")
        
        async with self._get_pool() as pool:
            # Validate scale before starting workers
            await self._validate_scale(pool, scale)
            
            async with asyncio.TaskGroup() as tg:
                for i in range(worker_count):
                    bid_from = math.floor(float(scale)/worker_count*i)+1
                    bid_to = math.floor(float(scale)/worker_count*(i+1))
                    worker = Worker(bid_from, bid_to, tran_count)
                    
                    if use_single_session:
                        tg.create_task(worker.execute_single_session(pool))
                    else:
                        tg.create_task(worker.execute_pooled(pool))
            logger.info("All workers completed")