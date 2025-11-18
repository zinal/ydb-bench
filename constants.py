"""
Constants for YDB pgbench implementation.

These values match the original pgbench specification and must remain
consistent between initialization and workload execution.
"""

# Number of tellers per branch (hardcoded in original pgbench)
TELLERS_PER_BRANCH = 10

# Number of accounts per branch (hardcoded in original pgbench)
ACCOUNTS_PER_BRANCH = 100000

# Default pgbench script
DEFAULT_SCRIPT = """
UPDATE `{table_folder}/accounts` SET abalance = abalance + $delta WHERE aid = $aid;
SELECT abalance FROM `{table_folder}/accounts` WHERE aid = $aid;
UPDATE `{table_folder}/tellers` SET tbalance = tbalance + $delta WHERE tid = $tid;
UPDATE `{table_folder}/branches` SET bbalance = bbalance + $delta WHERE bid = $bid;
INSERT INTO `{table_folder}/history` (tid, bid, aid, delta, mtime)
VALUES ($tid, $bid, $aid, $delta, CurrentUtcTimestamp());
"""