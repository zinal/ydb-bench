#!/usr/bin/env python3
import logging
import re
from multiprocessing import Pool
from typing import Any, Optional, Tuple

import click

from .runner import Runner

# Configure logging to stderr
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - PID:%(process)d - %(name)s - %(levelname)s - %(message)s",
    stream=__import__("sys").stderr,
)


def create_runner_from_config(
    endpoint: str,
    database: str,
    cert_file: Optional[str],
    user: Optional[str],
    password: Optional[str],
    table_folder: str,
) -> Runner:
    """
    Create a Runner instance from configuration.

    Args:
        endpoint: YDB endpoint
        database: Database path
        cert_file: Path to certificate file
        user: Username
        password: Password
        table_folder: Folder name for tables

    Returns:
        Runner instance
    """
    return Runner(
        endpoint=endpoint,
        database=database,
        root_certificates_file=cert_file,
        user=user,
        password=password,
        table_folder=table_folder,
    )


def _run_job_worker(
    args: Tuple[int, str, str, Optional[str], Optional[str], Optional[str], str, int, int, int, bool, Optional[str]],
) -> Optional[Any]:
    """
    Worker function for multiprocessing that runs a single job.
    Must be at module level to be picklable.

    Args:
        args: Tuple of (process_id, endpoint, database, ca_file, user, password,
              table_folder, jobs, transactions, scale, single_session, script)

    Returns:
        MetricsCollector instance with collected metrics, or None on error
    """
    import os
    import sys
    import traceback

    (
        process_id,
        endpoint,
        database,
        ca_file,
        user,
        password,
        table_folder,
        jobs,
        transactions,
        scale,
        single_session,
        script,
    ) = args

    pid = os.getpid()
    click.echo(f"Process {process_id} started (PID: {pid})")

    try:
        runner = create_runner_from_config(endpoint, database, ca_file, user, password, table_folder)
        metrics = runner.run(jobs, transactions, scale, single_session, script)
        return metrics
    except Exception as e:
        # Catch all exceptions to prevent unpicklable objects from being sent back
        # Print error to stderr and exit gracefully
        error_msg = f"Process {process_id} (PID: {pid}) failed with error: {str(e)}"
        print(error_msg, file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        # Don't re-raise to avoid pickle errors with protobuf objects
        return None


def validate_table_folder(_ctx: Any, _param: Any, table_folder: str) -> str:
    """
    Validate and sanitize table folder name to prevent SQL injection.
    """
    if not re.match(r"^[a-zA-Z0-9_\-\/]+$", table_folder):
        raise click.ClickException(
            f"Invalid table folder name '{table_folder}'. "
            "Only alphanumeric characters, underscores, hyphens and backslashes are allowed."
        )
    return table_folder


@click.group()
@click.option(
    "--endpoint",
    "-e",
    envvar="YDB_ENDPOINT",
    required=True,
    help="Endpoint to connect. (e.g., grpcs://host:2135)",
)
@click.option(
    "--database",
    "-d",
    envvar="YDB_DATABASE",
    required=True,
    help="Database to work with (e.g., /Root/database)",
)
@click.option("--ca-file", envvar="YDB_ROOT_CERT", help="Path to root certificate file")
@click.option("--user", envvar="YDB_USER", help="Username for authentication")
@click.option("--password", envvar="YDB_PASSWORD", help="Password for authentication")
@click.option(
    "--prefix-path",
    envvar="YDB_PREFIX_PATH",
    default="pgbench",
    callback=validate_table_folder,
    help="Folder name for tables (default: pgbench)",
)
@click.option(
    "--scale",
    "-s",
    type=int,
    default=100,
    help="Number of branches to create (default: 100)",
)
@click.pass_context
def cli(
    ctx: click.Context,
    endpoint: str,
    database: str,
    ca_file: Optional[str],
    user: Optional[str],
    password: Optional[str],
    prefix_path: str,
    scale: int,
) -> None:
    """YDB pgbench-like workload tool."""

    # Store common configuration in context
    ctx.ensure_object(dict)
    ctx.obj["endpoint"] = endpoint
    ctx.obj["database"] = database
    ctx.obj["ca_file"] = ca_file
    ctx.obj["user"] = user
    ctx.obj["password"] = password
    ctx.obj["prefix_path"] = prefix_path
    ctx.obj["scale"] = scale


@cli.command()
@click.pass_context
def init(ctx: click.Context) -> None:
    """Initialize database tables with test data."""
    # Get common configuration from context
    endpoint = ctx.obj["endpoint"]
    database = ctx.obj["database"]
    ca_file = ctx.obj["ca_file"]
    user = ctx.obj["user"]
    password = ctx.obj["password"]
    prefix_path = ctx.obj["prefix_path"]
    scale = ctx.obj["scale"]

    click.echo(f"Initializing database with prefix_path={prefix_path}, scale={scale}")

    runner = create_runner_from_config(endpoint, database, ca_file, user, password, prefix_path)
    runner.init_tables(scale)

    click.echo("Initialization completed")


@cli.command()
@click.option(
    "--client",
    "-c",
    type=int,
    default=1,
    help="Number of parallel client processes (default: 1)",
)
@click.option(
    "--jobs",
    "-j",
    type=int,
    default=1,
    help="Number of async jobs per process (default: 1)",
)
@click.option(
    "--transactions",
    "-t",
    type=int,
    default=100,
    help="Number of transactions each job runs (default: 100)",
)
@click.option(
    "--single-session",
    is_flag=True,
    help="Use single session mode instead of pooled mode",
)
@click.option(
    "--file",
    "-f",
    type=click.Path(exists=True, readable=True),
    help="Path to file containing SQL script to execute",
)
@click.pass_context
def run(
    ctx: click.Context,
    client: int,
    jobs: int,
    transactions: int,
    single_session: bool,
    file: Optional[str],
) -> None:
    """Run workload against the database."""
    # Get common configuration from context
    endpoint = ctx.obj["endpoint"]
    database = ctx.obj["database"]
    ca_file = ctx.obj["ca_file"]
    user = ctx.obj["user"]
    password = ctx.obj["password"]
    prefix_path = ctx.obj["prefix_path"]
    scale = ctx.obj["scale"]

    # Read script from file if provided
    script = None
    if file:
        with open(file, "r") as f:
            script = f.read()
        click.echo(f"Using script from file: {file}")

    mode = "single session" if single_session else "pooled"
    click.echo(
        f"Running workload with prefix_path={prefix_path}, scale={scale}, jobs={jobs}, transactions={transactions}, client={client}, mode={mode}"
    )

    if client == 1:
        # Single process execution
        runner = create_runner_from_config(endpoint, database, ca_file, user, password, prefix_path)
        metrics = runner.run(jobs, transactions, scale, single_session, script)
        # Print metrics for single process
        metrics.print_summary()
    else:
        # Multi-process execution
        from .metrics import MetricsCollector

        # Prepare arguments for each worker process
        worker_args = [
            (
                i,
                endpoint,
                database,
                ca_file,
                user,
                password,
                prefix_path,
                jobs,
                transactions,
                scale,
                single_session,
                script,
            )
            for i in range(client)
        ]

        with Pool(client) as pool:
            # Collect metrics from all worker processes
            results = pool.map(_run_job_worker, worker_args)

        # Merge all metrics into a single collector
        merged_metrics = MetricsCollector()
        for result in results:
            if result is not None:  # Skip failed processes
                merged_metrics.merge(result)

        # Print merged metrics once
        merged_metrics.print_summary()

    click.echo("Workload completed")


if __name__ == "__main__":
    cli()
