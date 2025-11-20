"""Parallel execution logic for running workloads across multiple processes."""
import sys
from multiprocessing import Pool
from typing import Optional

from .metrics import MetricsCollector
from .runner import Runner
from .workload import WeightedScriptSelector


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
    ) -> MetricsCollector:
        """
        Run workload with multiple processes in parallel.

        Args:
            processes: Number of parallel client processes
            jobs: Number of async jobs per process
            transactions: Number of transactions per job
            single_session: If True, use single session mode
            script_selector: Optional WeightedScriptSelector for multiple weighted scripts

        Returns:
            Merged MetricsCollector with results from all processes
        """
        # Prepare arguments for each worker process
        worker_args = [
            (i, jobs, transactions, single_session, script_selector)
            for i in range(processes)
        ]

        with Pool(processes) as pool:
            # Collect metrics from all worker processes
            results = pool.starmap(self.runner.run, worker_args)

        # Merge all metrics into a single collector
        merged_metrics = MetricsCollector()
        for result in results:
            if result is not None:  # Skip failed processes
                merged_metrics.merge(result)

        return merged_metrics