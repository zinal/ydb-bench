"""Parallel execution logic for running workloads across multiple processes."""

import sys
from multiprocessing import Pool
from typing import Optional

from .metrics import MetricsCollector
from .runner import Runner
from .workload import WeightedScriptSelector


def _run_worker(
    runner: Runner,
    process_id: int,
    jobs: int,
    transactions: int,
    single_session: bool,
    script_selector: Optional[WeightedScriptSelector],
    preheat: int,
) -> MetricsCollector:
    """Worker function that runs a runner instance."""
    return runner.run(process_id, jobs, transactions, single_session, script_selector, preheat)


class ParallelRunner:
    """Handles parallel execution of workloads across multiple processes."""

    def __init__(self, runner: Runner):
        """
        Initialize ParallelRunner with a Runner instance.

        Args:
            runner: Runner instance to use for workload execution
        """
        self.runner = runner

    def run_parallel(
        self,
        processes: int,
        jobs: int,
        transactions: int,
        single_session: bool,
        script_selector: Optional[WeightedScriptSelector] = None,
        preheat: int = 0,
    ) -> MetricsCollector:
        """
        Run workload with multiple processes in parallel.
        Each process gets its own non-overlapping bid range to avoid deadlocks.

        Args:
            processes: Number of parallel client processes
            jobs: Number of async jobs per process
            transactions: Number of transactions per job
            single_session: If True, use single session mode
            script_selector: Optional WeightedScriptSelector for multiple weighted scripts
            preheat: Number of preheat transactions to run before counting metrics (default: 0)

        Returns:
            Merged MetricsCollector with results from all processes
        """
        # Split runner into multiple non-overlapping copies
        runners = self.runner.split(processes)

        # Prepare arguments for each worker process
        worker_args = [
            (runner, i, jobs, transactions, single_session, script_selector, preheat)
            for i, runner in enumerate(runners)
        ]

        with Pool(processes) as pool:
            # Collect metrics from all worker processes
            results = pool.starmap(_run_worker, worker_args)

        # Merge all metrics into a single collector
        merged_metrics = MetricsCollector()
        for result in results:
            if result is not None:  # Skip failed processes
                merged_metrics.merge(result)

        return merged_metrics
