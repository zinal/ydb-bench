"""Parallel execution logic for running workloads across multiple processes."""

import sys
from multiprocessing import Pool
from typing import Optional
import time

from .metrics import MetricsCollector
from .runner import Runner
from .workload import WeightedScriptSelector
from .constants import DurationUnit


def _run_worker(
    runner: Runner,
    workload_start_time: float,
    duration: int,
    duration_unit: DurationUnit,
    process_id: int,
    jobs: int,
    single_session: bool,
    script_selector: Optional[WeightedScriptSelector],
    skip_scale_validation: bool,
) -> MetricsCollector:
    """Worker function that runs a runner instance."""
    return runner.run(workload_start_time, duration, duration_unit, process_id, jobs, single_session, script_selector, skip_scale_validation)


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
        workload_start_time: float,
        duration: int,
        duration_unit: DurationUnit,
        processes: int,
        jobs: int,
        single_session: bool,
        script_selector: Optional[WeightedScriptSelector] = None,
        skip_scale_validation: bool = False,
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
            skip_scale_validation: If True, skip scale validation (useful for custom workloads)

        Returns:
            Merged MetricsCollector with results from all processes
        """

        # Split runner into multiple non-overlapping copies
        runners = self.runner.split(processes)

        # Prepare arguments for each worker process
        worker_args = [
            (runner, workload_start_time, duration, duration_unit, i, jobs, single_session, script_selector, skip_scale_validation)
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
