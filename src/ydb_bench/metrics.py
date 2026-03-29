import logging
import sys
import time
import statistics
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class TransactionMetrics:
    """Metrics for a single transaction."""

    filepath: str
    start_time: float
    end_time: float
    success: bool
    error_message: str = ""
    server_duration_us: int = 0
    server_cpu_time_us: int = 0

    @property
    def latency(self) -> float:
        """Transaction latency in seconds."""
        return self.end_time - self.start_time

    @property
    def server_duration_ms(self) -> float:
        """Server-side duration in milliseconds."""
        return self.server_duration_us / 1000.0

    @property
    def server_cpu_time_ms(self) -> float:
        """Server-side CPU time in milliseconds."""
        return self.server_cpu_time_us / 1000.0


@dataclass
class MetricsCollector:
    """Collector for transaction metrics. Safe for use with asyncio (single-threaded)."""

    transactions: List[TransactionMetrics] = field(default_factory=list)
    _start_time: Optional[float] = None
    unhandled_error_messages: List[str] = field(default_factory=list)

    def record_transaction(
        self,
        filepath: str,
        start_time: float,
        end_time: float,
        success: bool,
        error_message: str = "",
        server_duration_us: int = 0,
        server_cpu_time_us: int = 0,
    ) -> None:
        """
        Record a transaction's metrics.

        Args:
            start_time: Transaction start timestamp
            end_time: Transaction end timestamp
            success: Whether the transaction succeeded
            error_message: Error message if transaction failed
            server_duration_us: Server-side total duration in microseconds
            server_cpu_time_us: Server-side CPU time in microseconds
        """
        if self._start_time is None:
            self._start_time = time.time()
        self.transactions.append(
            TransactionMetrics(
                filepath=filepath,
                start_time=start_time,
                end_time=end_time,
                success=success,
                error_message=error_message,
                server_duration_us=server_duration_us,
                server_cpu_time_us=server_cpu_time_us,
            )
        )

    def merge(self, other: "MetricsCollector") -> None:
        """
        Merge transactions from another MetricsCollector into this one.

        Args:
            other: Another MetricsCollector instance to merge from
        """
        self.transactions.extend(other.transactions)
        self.unhandled_error_messages.extend(other.unhandled_error_messages)
        # Update start time to the earliest one
        if self._start_time is None or (other._start_time is not None and other._start_time < self._start_time):
            self._start_time = other._start_time

    def _calculate_percentiles(self, values: List[float]) -> Dict[str, float]:
        """Calculate percentiles for a list of values."""
        if not values:
            return {
                "avg": 0.0,
                "stddev": 0.0,
                "min": 0.0,
                "max": 0.0,
                "p50": 0.0,
                "p95": 0.0,
                "p99": 0.0,
            }

        sorted_values = sorted(values)
        avg = sum(sorted_values) / len(sorted_values)
        # stdev requires at least 2 data points
        stddev = statistics.stdev(sorted_values) if len(sorted_values) > 1 else 0.0
        min_val = sorted_values[0]
        max_val = sorted_values[-1]

        p50_index = int(len(sorted_values) * 0.50)
        p95_index = int(len(sorted_values) * 0.95)
        p99_index = int(len(sorted_values) * 0.99)

        p50 = sorted_values[p50_index] if p50_index < len(sorted_values) else sorted_values[-1]
        p95 = sorted_values[p95_index] if p95_index < len(sorted_values) else sorted_values[-1]
        p99 = sorted_values[p99_index] if p99_index < len(sorted_values) else sorted_values[-1]

        return {
            "avg": avg,
            "stddev": stddev,
            "min": min_val,
            "max": max_val,
            "p50": p50,
            "p95": p95,
            "p99": p99,
        }

    def get_summary(self, workload: str) -> Dict[str, Any]:
        """
        Calculate and return summary statistics.

        Returns:
            Dictionary containing metrics summary
        """
        if not self.transactions:
            return {
                "total_duration": 0.0,
                "total_transactions": 0,
                "successful_transactions": 0,
                "failed_transactions": 0,
                "tps": 0.0,
                "latency": {},
                "server_duration": {},
                "server_cpu_time": {},
            }

        # Значение, которое мы ищем в поле filepath
        target_filepath = workload
        # Получаем список объектов, у которых поле filepath равно target_filepath или j,ob
        if target_filepath == "SUMMARY":
            filtered_transactions = self.transactions
        else:
            filtered_transactions = [
                transaction for transaction in self.transactions if transaction.filepath == target_filepath
            ]

        # Check if filtered transactions is empty
        if not filtered_transactions:
            return {
                "total_duration": 0.0,
                "total_transactions": 0,
                "successful_transactions": 0,
                "failed_transactions": 0,
                "tps": 0.0,
                "latency": {},
                "server_duration": {},
                "server_cpu_time": {},
            }

        start_times = [t.start_time for t in filtered_transactions if t.success and t.server_duration_us > 0]
        end_times = [t.end_time for t in filtered_transactions if t.success and t.server_cpu_time_us > 0]
        
        # Handle case where no successful transactions with timing data exist
        if not start_times or not end_times:
            # Fall back to using all transaction times
            start_times = [t.start_time for t in filtered_transactions]
            end_times = [t.end_time for t in filtered_transactions]
        
        if not start_times or not end_times:
            # Still no data - return empty summary
            return {
                "total_duration": 0.0,
                "total_transactions": len(filtered_transactions),
                "successful_transactions": sum(1 for t in filtered_transactions if t.success),
                "failed_transactions": sum(1 for t in filtered_transactions if not t.success),
                "tps": 0.0,
                "latency": {},
                "server_duration": {},
                "server_cpu_time": {},
            }
        
        min_time = start_times[0]
        max_time = end_times[-1]

        total_duration = max_time - min_time

        total_transactions = len(filtered_transactions)
        successful_transactions = sum(1 for t in filtered_transactions if t.success)
        failed_transactions = total_transactions - successful_transactions

        # Calculate client-side latency statistics (in milliseconds)
        latencies_ms = [t.latency * 1000 for t in filtered_transactions]
        latency_stats = self._calculate_percentiles(latencies_ms)

        # Calculate server-side metrics (only for successful transactions with stats)
        server_durations = [
            t.server_duration_ms for t in filtered_transactions if t.success and t.server_duration_us > 0
        ]
        server_cpu_times = [
            t.server_cpu_time_ms for t in filtered_transactions if t.success and t.server_cpu_time_us > 0
        ]

        server_duration_stats = self._calculate_percentiles(server_durations)
        server_cpu_time_stats = self._calculate_percentiles(server_cpu_times)

        # Calculate transactions per second
        tps = total_transactions / total_duration if total_duration > 0 else 0.0

        return {
            "total_duration": total_duration,
            "total_transactions": total_transactions,
            "successful_transactions": successful_transactions,
            "failed_transactions": failed_transactions,
            "tps": tps,
            "latency": latency_stats,
            "server_duration": server_duration_stats,
            "server_cpu_time": server_cpu_time_stats,
        }

    def print_group(self, workload: str) -> None:
        """Print formatted metrics summary to stdout (not as log)."""

        summary = self.get_summary(workload)

        # Print directly to stdout, not through logger
        print("=" * 90, file=sys.stdout)
        print(f"PERFORMANCE METRICS: {workload}", file=sys.stdout)
        print("=" * 90, file=sys.stdout)
        print(
            f"Total Duration:           {summary['total_duration']:.2f} seconds",
            file=sys.stdout,
        )
        print(
            f"Total Transactions:       {summary['total_transactions']}",
            file=sys.stdout,
        )
        print(
            f"Successful Transactions:  {summary['successful_transactions']}",
            file=sys.stdout,
        )
        print(
            f"Failed Transactions:      {summary['failed_transactions']}",
            file=sys.stdout,
        )
        print(f"Transactions per Second:  {summary['tps']:.2f} TPS", file=sys.stdout)
        print("=" * 90, file=sys.stdout)

        # Only print detailed metrics if we have data
        lat = summary["latency"]
        srv_dur = summary["server_duration"]
        srv_cpu = summary["server_cpu_time"]
        
        if not lat or not srv_dur or not srv_cpu:
            print("\nNo transaction data available for detailed metrics.", file=sys.stdout)
            print("=" * 90, file=sys.stdout)
            return

        # Print table header
        print(
            f"{'Metric':<15} {'Client duration (ms)':>20} {'Server Duration (ms)':>25} {'CPU Time (ms)':>20}",
            file=sys.stdout,
        )
        print("-" * 90, file=sys.stdout)

        # Print statistics rows
        print(
            f"{'Average':<15} {lat['avg']:>20.2f} {srv_dur['avg']:>25.2f} {srv_cpu['avg']:>20.2f}",
            file=sys.stdout,
        )
        print(
            f"{'STDDev':<15} {lat['stddev']:>20.2f} {srv_dur['stddev']:>25.2f} {srv_cpu['stddev']:>20.2f}",
            file=sys.stdout,
        )
        print()
        print(
            f"{'Minimum':<15} {lat['min']:>20.2f} {srv_dur['min']:>25.2f} {srv_cpu['min']:>20.2f}",
            file=sys.stdout,
        )
        print(
            f"{'Maximum':<15} {lat['max']:>20.2f} {srv_dur['max']:>25.2f} {srv_cpu['max']:>20.2f}",
            file=sys.stdout,
        )
        print()
        print(
            f"{'P50 (Median)':<15} {lat['p50']:>20.2f} {srv_dur['p50']:>25.2f} {srv_cpu['p50']:>20.2f}",
            file=sys.stdout,
        )
        print(
            f"{'P95':<15} {lat['p95']:>20.2f} {srv_dur['p95']:>25.2f} {srv_cpu['p95']:>20.2f}",
            file=sys.stdout,
        )
        print(
            f"{'P99':<15} {lat['p99']:>20.2f} {srv_dur['p99']:>25.2f} {srv_cpu['p99']:>20.2f}",
            file=sys.stdout,
        )

        print("=" * 90, file=sys.stdout)

        # Print any unhandled errors
        if self.unhandled_error_messages:
            print("\nUnhandled errors occurred:", file=sys.stderr)
            for error_msg in self.unhandled_error_messages:
                print(f"  {error_msg}", file=sys.stderr)
            print("=" * 90, file=sys.stdout)

        sys.stdout.flush()

    def print_summary(self) -> None:
        """Print summary of all metrics, including any errors that occurred."""
        
        # First, check if there were any unhandled errors
        if self.unhandled_error_messages:
            print("\n" + "=" * 90, file=sys.stderr)
            print("ERROR: Workload execution failed", file=sys.stderr)
            print("=" * 90, file=sys.stderr)
            for error_msg in self.unhandled_error_messages:
                print(f"{error_msg}", file=sys.stderr)
            print("=" * 90, file=sys.stderr)
            print("\nCommon causes:", file=sys.stderr)
            print("  - Scale parameter exceeds initialized data (run 'init' with appropriate --scale)", file=sys.stderr)
            print("  - Database tables not initialized (run 'init' command first)", file=sys.stderr)
            print("  - Connection or permission issues", file=sys.stderr)
            print("=" * 90 + "\n", file=sys.stderr)
            sys.stderr.flush()

        # Check if we have any transactions
        if not self.transactions:
            print("\n" + "=" * 90, file=sys.stdout)
            print("WARNING: No transactions were executed", file=sys.stdout)
            print("=" * 90, file=sys.stdout)
            print("\nPossible causes:", file=sys.stdout)
            print("  - Database tables not initialized (run 'init' command first)", file=sys.stdout)
            print("  - Scale parameter exceeds initialized data", file=sys.stdout)
            print("  - Workload duration too short", file=sys.stdout)
            print("=" * 90 + "\n", file=sys.stdout)
            sys.stdout.flush()
            return

        unique_filepaths = sorted({transaction.filepath for transaction in self.transactions})
        self.print_group("SUMMARY")
        count_unique_filepaths = len(unique_filepaths)
        if count_unique_filepaths > 1:
            for filepath in unique_filepaths:
                print()
                print()
                self.print_group(filepath)
