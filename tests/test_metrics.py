"""
Tests for metrics error handling improvements.
"""
import time
import pytest
from io import StringIO
import sys

from ydb_bench.metrics import MetricsCollector, TransactionMetrics


class TestMetricsCollector:
    """Test MetricsCollector error handling."""

    def test_empty_transactions_get_summary(self):
        """Test that get_summary handles empty transactions gracefully."""
        metrics = MetricsCollector()
        summary = metrics.get_summary("SUMMARY")
        
        assert summary["total_transactions"] == 0
        assert summary["successful_transactions"] == 0
        assert summary["failed_transactions"] == 0
        assert summary["tps"] == 0.0
        assert summary["latency"] == {}
        assert summary["server_duration"] == {}
        assert summary["server_cpu_time"] == {}

    def test_empty_transactions_print_summary(self, capsys):
        """Test that print_summary handles empty transactions without crashing."""
        metrics = MetricsCollector()
        
        # Should not raise an exception
        metrics.print_summary()
        
        captured = capsys.readouterr()
        assert "WARNING: No transactions were executed" in captured.out
        assert "Database tables not initialized" in captured.out

    def test_error_message_display(self, capsys):
        """Test that error messages are displayed properly."""
        metrics = MetricsCollector()
        metrics.unhandled_error_messages.append(
            "Process 0 (PID: 12345) failed with error: Scale validation failed"
        )
        
        metrics.print_summary()
        
        captured = capsys.readouterr()
        assert "ERROR: Workload execution failed" in captured.err
        assert "Scale validation failed" in captured.err
        assert "Common causes:" in captured.err

    def test_empty_filtered_transactions(self):
        """Test that get_summary handles empty filtered transactions."""
        metrics = MetricsCollector()
        
        # Add a transaction with specific filepath
        metrics.record_transaction(
            filepath="test.sql",
            start_time=time.time(),
            end_time=time.time() + 0.1,
            success=True,
            server_duration_us=50000,
            server_cpu_time_us=30000
        )
        
        # Try to get summary for non-existent workload
        summary = metrics.get_summary("nonexistent.sql")
        
        assert summary["total_transactions"] == 0
        assert summary["latency"] == {}
        assert summary["server_duration"] == {}
        assert summary["server_cpu_time"] == {}

    def test_transactions_without_timing_data(self):
        """Test handling of transactions without server timing data."""
        metrics = MetricsCollector()
        
        # Add transactions without server timing data
        start = time.time()
        metrics.record_transaction(
            filepath="test.sql",
            start_time=start,
            end_time=start + 0.1,
            success=True,
            server_duration_us=0,  # No server timing
            server_cpu_time_us=0
        )
        
        summary = metrics.get_summary("SUMMARY")
        
        # Should still calculate basic metrics
        assert summary["total_transactions"] == 1
        assert summary["successful_transactions"] == 1
        assert summary["total_duration"] > 0

    def test_print_group_with_no_data(self, capsys):
        """Test that print_group handles empty data gracefully."""
        metrics = MetricsCollector()
        
        # Should not crash when printing group with no data
        metrics.print_group("SUMMARY")
        
        captured = capsys.readouterr()
        assert "PERFORMANCE METRICS: SUMMARY" in captured.out
        assert "No transaction data available" in captured.out

    def test_successful_transactions_with_data(self):
        """Test normal case with successful transactions."""
        metrics = MetricsCollector()
        
        start = time.time()
        for i in range(10):
            metrics.record_transaction(
                filepath="test.sql",
                start_time=start + i * 0.1,
                end_time=start + i * 0.1 + 0.05,
                success=True,
                server_duration_us=40000,
                server_cpu_time_us=25000
            )
        
        summary = metrics.get_summary("SUMMARY")
        
        assert summary["total_transactions"] == 10
        assert summary["successful_transactions"] == 10
        assert summary["failed_transactions"] == 0
        assert summary["tps"] > 0
        assert "avg" in summary["latency"]
        assert "avg" in summary["server_duration"]
        assert "avg" in summary["server_cpu_time"]

    def test_mixed_success_and_failure(self):
        """Test transactions with mixed success/failure."""
        metrics = MetricsCollector()
        
        start = time.time()
        # Add successful transactions
        for i in range(5):
            metrics.record_transaction(
                filepath="test.sql",
                start_time=start + i * 0.1,
                end_time=start + i * 0.1 + 0.05,
                success=True,
                server_duration_us=40000,
                server_cpu_time_us=25000
            )
        
        # Add failed transactions
        for i in range(5, 8):
            metrics.record_transaction(
                filepath="test.sql",
                start_time=start + i * 0.1,
                end_time=start + i * 0.1 + 0.05,
                success=False,
                error_message="Test error"
            )
        
        summary = metrics.get_summary("SUMMARY")
        
        assert summary["total_transactions"] == 8
        assert summary["successful_transactions"] == 5
        assert summary["failed_transactions"] == 3

    def test_merge_collectors(self):
        """Test merging multiple collectors."""
        metrics1 = MetricsCollector()
        metrics2 = MetricsCollector()
        
        start = time.time()
        metrics1.record_transaction(
            filepath="test1.sql",
            start_time=start,
            end_time=start + 0.1,
            success=True,
            server_duration_us=40000,
            server_cpu_time_us=25000
        )
        
        metrics2.record_transaction(
            filepath="test2.sql",
            start_time=start + 0.2,
            end_time=start + 0.3,
            success=True,
            server_duration_us=50000,
            server_cpu_time_us=30000
        )
        
        metrics1.merge(metrics2)
        
        assert len(metrics1.transactions) == 2
        summary = metrics1.get_summary("SUMMARY")
        assert summary["total_transactions"] == 2

    def test_multiple_workloads(self, capsys):
        """Test print_summary with multiple different workloads."""
        metrics = MetricsCollector()
        
        start = time.time()
        # Add transactions for different workloads
        for filepath in ["workload1.sql", "workload2.sql"]:
            for i in range(3):
                metrics.record_transaction(
                    filepath=filepath,
                    start_time=start + i * 0.1,
                    end_time=start + i * 0.1 + 0.05,
                    success=True,
                    server_duration_us=40000,
                    server_cpu_time_us=25000
                )
        
        metrics.print_summary()
        
        captured = capsys.readouterr()
        # Should print summary and individual workload metrics
        assert "PERFORMANCE METRICS: SUMMARY" in captured.out
        assert "PERFORMANCE METRICS: workload1.sql" in captured.out
        assert "PERFORMANCE METRICS: workload2.sql" in captured.out
