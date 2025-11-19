import logging
import sys
import time
from dataclasses import dataclass, field
from typing import List

logger = logging.getLogger(__name__)


@dataclass
class TransactionMetrics:
    """Metrics for a single transaction."""

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
    _start_time: float = field(default_factory=time.time)

    def record_transaction(
        self,
        start_time: float,
        end_time: float,
        success: bool,
        error_message: str = "",
        server_duration_us: int = 0,
        server_cpu_time_us: int = 0,
    ):
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
        self.transactions.append(
            TransactionMetrics(
                start_time=start_time,
                end_time=end_time,
                success=success,
                error_message=error_message,
                server_duration_us=server_duration_us,
                server_cpu_time_us=server_cpu_time_us,
            )
        )

    def merge(self, other: "MetricsCollector"):
        """
        Merge transactions from another MetricsCollector into this one.

        Args:
            other: Another MetricsCollector instance to merge from
        """
        self.transactions.extend(other.transactions)
        # Update start time to the earliest one
        if other._start_time < self._start_time:
            self._start_time = other._start_time

    def _calculate_percentiles(self, values: List[float]) -> dict:
        """Calculate percentiles for a list of values."""
        if not values:
            return {
                "avg": 0.0,
                "min": 0.0,
                "max": 0.0,
                "p50": 0.0,
                "p95": 0.0,
                "p99": 0.0,
            }

        sorted_values = sorted(values)
        avg = sum(sorted_values) / len(sorted_values)
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
            "min": min_val,
            "max": max_val,
            "p50": p50,
            "p95": p95,
            "p99": p99,
        }

    def get_summary(self) -> dict:
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

        total_duration = time.time() - self._start_time
        total_transactions = len(self.transactions)
        successful_transactions = sum(1 for t in self.transactions if t.success)
        failed_transactions = total_transactions - successful_transactions

        # Calculate client-side latency statistics (in milliseconds)
        latencies_ms = [t.latency * 1000 for t in self.transactions]
        latency_stats = self._calculate_percentiles(latencies_ms)

        # Calculate server-side metrics (only for successful transactions with stats)
        server_durations = [t.server_duration_ms for t in self.transactions if t.success and t.server_duration_us > 0]
        server_cpu_times = [t.server_cpu_time_ms for t in self.transactions if t.success and t.server_cpu_time_us > 0]

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

    def print_summary(self):
        """Print formatted metrics summary to stdout (not as log)."""
        summary = self.get_summary()

        # Print directly to stdout, not through logger
        print("=" * 90, file=sys.stdout)
        print("PERFORMANCE METRICS SUMMARY", file=sys.stdout)
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

        # Print table header
        print(
            f"{'Metric':<15} {'Client duration (ms)':>20} {'Server Duration (ms)':>25} {'CPU Time (ms)':>20}",
            file=sys.stdout,
        )
        print("-" * 90, file=sys.stdout)

        # Print statistics rows
        lat = summary["latency"]
        srv_dur = summary["server_duration"]
        srv_cpu = summary["server_cpu_time"]

        print(
            f"{'Average':<15} {lat['avg']:>20.2f} {srv_dur['avg']:>25.2f} {srv_cpu['avg']:>20.2f}",
            file=sys.stdout,
        )
        print(
            f"{'Minimum':<15} {lat['min']:>20.2f} {srv_dur['min']:>25.2f} {srv_cpu['min']:>20.2f}",
            file=sys.stdout,
        )
        print(
            f"{'Maximum':<15} {lat['max']:>20.2f} {srv_dur['max']:>25.2f} {srv_cpu['max']:>20.2f}",
            file=sys.stdout,
        )
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
        sys.stdout.flush()
