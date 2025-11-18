import ydb
from typing import Optional
from constants import TELLERS_PER_BRANCH, ACCOUNTS_PER_BRANCH
from metrics import MetricsCollector
from base_executor import BaseExecutor


class Initializer(BaseExecutor):
    """
    Initializes pgbench database schema and data.
    
    Uses sequential branch processing within the range.
    """
    
    def __init__(
        self,
        bid_from: int,
        bid_to: int,
        metrics_collector: Optional[MetricsCollector] = None,
        table_folder: str = "pgbench",
        use_single_session: bool = False
    ):
        """
        Initialize Initializer with branch range.
        
        Args:
            bid_from: Starting branch ID (inclusive)
            bid_to: Ending branch ID (inclusive)
            metrics_collector: Optional metrics collector
            table_folder: Folder name for tables
            use_single_session: If True, use single session mode; if False, use pooled mode
        """
        count = bid_to - bid_from + 1
        super().__init__(bid_from, bid_to, count, metrics_collector, table_folder, use_single_session)

    async def create_tables(self, pool: ydb.aio.QuerySessionPool):
        """Create the pgbench tables in the database."""
        await pool.execute_with_retries(
            f"""
            DROP TABLE IF EXISTS `{self._table_folder}/accounts`;
            CREATE TABLE `{self._table_folder}/accounts`
            (
                aid Int32,
                bid Int32,
                abalance Int32,
                filler Utf8,
                PRIMARY KEY(aid)
            );

            DROP TABLE IF EXISTS `{self._table_folder}/branches`;
            CREATE TABLE `{self._table_folder}/branches`
            (
                bid Int32,
                bbalance Int32,
                filler Utf8,
                PRIMARY KEY(bid)
            );

            DROP TABLE IF EXISTS `{self._table_folder}/tellers`;
            CREATE TABLE `{self._table_folder}/tellers`
            (
                tid Int32,
                bid Int32,
                tbalance Int32,
                filler Utf8,
                PRIMARY KEY(tid)
            );

            DROP TABLE IF EXISTS `{self._table_folder}/history`;
            CREATE TABLE `{self._table_folder}/history`
            (
                tid Int32,
                bid Int32,
                aid Int32,
                delta Int32,
                mtime timestamp,
                filler Utf8,
                PRIMARY KEY(aid, mtime)
            );
            """
        )

    async def _execute_operation(self, session: ydb.aio.QuerySession, iteration: int):
        """
        Fill data for a single branch.
        
        Args:
            session: YDB query session
            iteration: Current iteration number (0-based)
        """
        bid = self._bid_from + iteration
        
        async with session.transaction() as tx:
            await tx.execute(
                f"""
                $d = SELECT d FROM (SELECT AsList(0,1,2,3,4,5,6,7,8,9) as d) FLATTEN LIST BY (d);

                REPLACE INTO `{self._table_folder}/branches`(bid, bbalance, filler)
                VALUES ($bid, 0 , null);

                REPLACE INTO `{self._table_folder}/tellers`(tid, bid, tbalance, filler)
                SELECT
                    ($bid-1)*$tellers_per_branch+d1.d+1 as tid, $bid, 0 , null
                FROM
                    $d as d1;

                REPLACE INTO `{self._table_folder}/accounts`(aid, bid, abalance, filler)
                SELECT
                    ($bid-1)*$accounts_per_branch + rn + 1 as aid,
                    $bid as bid,
                    0 as abalance,
                    null as filler
                FROM (
                    SELECT
                        d1.d+d2.d*10+d3.d*100+d4.d*1000+d5.d*10000 as rn
                    FROM
                        -- 100k rows
                        $d as d1
                        CROSS JOIN $d as d2
                        CROSS JOIN $d as d3
                        CROSS JOIN $d as d4
                        CROSS JOIN $d as d5
                    ) t
                """,
                parameters={
                        "$bid": ydb.TypedValue(bid, ydb.PrimitiveType.Int32),
                        "$tellers_per_branch": ydb.TypedValue(TELLERS_PER_BRANCH, ydb.PrimitiveType.Int32),
                        "$accounts_per_branch": ydb.TypedValue(ACCOUNTS_PER_BRANCH, ydb.PrimitiveType.Int32)
                },
                commit_tx=True
            )