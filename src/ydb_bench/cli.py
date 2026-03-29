#!/usr/bin/env python3
import logging
import os
import re
import sys
from typing import Any, List, Optional, Tuple
import time

import click
from click_option_group import optgroup, MutuallyExclusiveOptionGroup

from .parallel_runner import ParallelRunner
from .runner import Runner
from .workload import WeightedScriptSelector, WorkloadScript
from .constants import DurationUnit


def setup_logging(log_level_str: str) -> None:
    """Converts logging level string to numeric level and configures logging."""
    # Словарь допустимых уровней (можно расширить)
    level_map = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL,
    }

    if log_level_str not in level_map:
        raise ValueError(
            f"Invalid logging level: {log_level_str}. " f"Valid values: {list(level_map.keys())}"
        )

    log_level = level_map[log_level_str]

    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - PID:%(process)d - %(name)s - %(levelname)s - %(message)s",
        stream=sys.stderr,
    )


def parse_weighted_file_spec(_ctx: Any, _param: Any, values: str) -> List[Tuple[str, float]]:
    """
    Parse a file specification in format 'filename.sql@weight' or 'filename.sql'.

    This is a Click callback that validates and parses the file specification.

    Args:
        _ctx: Click context (unused)
        _param: Click parameter (unused)
        value: File specification string

    Returns:
        Tuple of (filepath, weight)

    Raises:
        click.BadParameter: If weight syntax is invalid
    """
    result = []
    for value in values:
        if "@" in value:
            filepath, weight_str = value.rsplit("@", 1)
            try:
                weight = float(weight_str)
                if weight <= 0:
                    raise click.BadParameter(f"Weight must be positive in: {value}")
            except ValueError:
                raise click.BadParameter(f"Invalid weight syntax in: {value}. Expected format: file.sql@weight")
        else:
            filepath = value
            weight = 1.0

        result.append((filepath, weight))
    return result


def parse_weighted_builtin_spec(_ctx: Any, _param: Any, values: str) -> List[Tuple[str, float]]:
    """
    Parse a builtin specification in format 'NAME@weight' or 'NAME'.

    This is a Click callback that validates and parses the builtin specification.

    Args:
        _ctx: Click context (unused)
        _param: Click parameter (unused)
        values: Builtin specification strings

    Returns:
        List of tuples of (builtin_name, weight)

    Raises:
        click.BadParameter: If weight syntax is invalid or builtin name is unknown
    """
    result = []
    valid_builtins = ["tpcb-like"]

    for value in values:
        if "@" in value:
            builtin_name, weight_str = value.rsplit("@", 1)
            try:
                weight = float(weight_str)
                if weight <= 0:
                    raise click.BadParameter(f"Weight must be positive in: {value}")
            except ValueError:
                raise click.BadParameter(f"Invalid weight syntax in: {value}. Expected format: NAME@weight")
        else:
            builtin_name = value
            weight = 1.0

        if builtin_name not in valid_builtins:
            raise click.BadParameter(
                f"Unknown builtin name: {builtin_name}. Valid options: {', '.join(valid_builtins)}"
            )

        result.append((builtin_name, weight))
    return result


def create_workload_script(filepath: str, weight: float, table_folder: str) -> WorkloadScript:
    """
    Create a WorkloadScript from a file path and weight.

    Args:
        filepath: Path to SQL file
        weight: Weight for random selection
        table_folder: Table folder name for script formatting

    Returns:
        WorkloadScript instance

    Raises:
        click.ClickException: If file doesn't exist or can't be read
    """
    # Validate file exists
    if not os.path.exists(filepath):
        raise click.ClickException(f"File not found: {filepath}")

    # Read file content
    try:
        with open(filepath, "r") as f:
            content = f.read()
    except Exception as e:
        raise click.ClickException(f"Error reading file {filepath}: {str(e)}")

    # Create WorkloadScript
    return WorkloadScript(filepath, content, weight, table_folder)


def create_script_selector(
    file_specs: Tuple[Tuple[str, float], ...], builtin_specs: Tuple[Tuple[str, float], ...], table_folder: str
) -> Optional[WeightedScriptSelector]:
    """
    Create a WeightedScriptSelector from file and builtin specifications.

    Args:
        file_specs: Tuple of (filepath, weight) tuples
        builtin_specs: Tuple of (builtin_name, weight) tuples
        table_folder: Table folder name for script formatting

    Returns:
        WeightedScriptSelector instance if files or builtins provided, None otherwise
    """
    from .constants import DEFAULT_SCRIPT

    scripts = []

    # Add builtin scripts
    if builtin_specs:
        for builtin_name, weight in builtin_specs:
            if builtin_name == "tpcb-like":
                script = WorkloadScript(
                    filepath=f"<builtin:{builtin_name}>",
                    content=DEFAULT_SCRIPT,
                    weight=weight,
                    table_folder=table_folder,
                )
                scripts.append(script)
                click.echo(f"Loaded builtin: {builtin_name} (weight: {weight})")

    # Add file scripts
    if file_specs:
        for filepath, weight in file_specs:
            script = create_workload_script(filepath, weight, table_folder)
            scripts.append(script)
            click.echo(f"Loaded script: {script.filepath} (weight: {script.weight})")

    if not scripts:
        return None

    # Create selector
    script_selector = WeightedScriptSelector(scripts)
    total_weight = script_selector.total_weight
    click.echo(f"Total weight: {total_weight}")

    return script_selector


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
@click.option(
    "--log-level",
    type=str,
    default="INFO",
    help="Logging level. Opdions: DEBUG, INFO, WARNING,ERROR, CRITICAL. Default: INFO",
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
    log_level: str,
) -> None:
    """YDB pgbench-like workload tool."""

    # Настройка уровня логирования
    setup_logging(log_level)

    # Create Runner instance and store in context
    # Convert scale to bid_from and bid_to
    runner = Runner(
        endpoint=endpoint,
        database=database,
        bid_from=1,
        bid_to=scale,
        root_certificates_file=ca_file,
        user=user,
        password=password,
        table_folder=prefix_path,
    )
    # Test connection
    runner.test_connection()

    ctx.ensure_object(dict)
    ctx.obj["runner"] = runner
    # Store scale for display purposes
    ctx.obj["scale"] = scale


@cli.command()
@click.option(
    "--file",
    "-f",
    multiple=False,
    type=str,
    help="Path to SQL file with tables DDL definition",
)
@click.pass_context
def init(ctx: click.Context, file: str) -> None:
    """Initialize database tables with test data."""
    runner = ctx.obj["runner"]
    scale = ctx.obj["scale"]

    click.echo(f"Initializing database with prefix_path={runner.table_folder}, scale={scale}, ddl_file={file}")

    runner.init_tables(file)

    click.echo("Initialization completed")


@cli.command()
@click.option(
    "--processes",
    "-p",
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
    "--preheat-duration",
    "-P",
    type=int,
    default=30,
    help="Preheat duration time in seconds (default: 30)",
)
@optgroup.group(
    "Workload parameters",
    cls=MutuallyExclusiveOptionGroup,
    help="Specify ONLY one parameter: number of transactions OR workload duration.",
)
@optgroup.option(
    "--transactions",
    "-t",
    type=int,
    help="Number of transactions each job runs (default: 10)",
)
@optgroup.option(
    "--workload-duration",
    "-T",
    type=int,
    default=120,
    help="Workload duration time in seconds (default: 120)",
)
@click.option(
    "--single-session",
    is_flag=True,
    help="Use single session mode instead of pooled mode",
)
@click.option(
    "--file",
    "-f",
    multiple=True,
    type=str,
    callback=parse_weighted_file_spec,
    help="Path to SQL file with optional weight: file.sql@weight (default weight: 1). Can be specified multiple times.",
)
@click.option(
    "--builtin",
    "-b",
    multiple=True,
    type=str,
    callback=parse_weighted_builtin_spec,
    help="Add builtin script NAME with optional weight (default: 1). Format: NAME@weight. Currently supported: tpcb-like. Can be specified multiple times.",
)
@click.option(
    "--no-validate-scale",
    is_flag=True,
    help="Disable scale validation (useful for custom workloads that don't use standard tables)",
)
@click.pass_context
def run(
    ctx: click.Context,
    processes: int,
    jobs: int,
    preheat_duration: int,
    transactions: int,
    workload_duration: int,
    single_session: bool,
    file: Tuple[Tuple[str, float], ...],
    builtin: Tuple[Tuple[str, float], ...],
    no_validate_scale: bool,
) -> None:
    """Run workload against the database."""
    runner = ctx.obj["runner"]
    scale = ctx.obj["scale"]

    # If neither --file nor --builtin specified, default to builtin tpcb-like
    if not file and not builtin:
        builtin = (("tpcb-like", 1.0),)

    # Create script selector from parsed file and builtin specifications
    script_selector = create_script_selector(file, builtin, runner.table_folder)

    mode = "single session" if single_session else "pooled"

    if preheat_duration < 0:
        raise ValueError("`preheat_duration` value can't be negative")

    if workload_duration < 0:
        raise ValueError("`workload_duration` value can't be negative")

    workload_start_time = time.time() + preheat_duration

    if transactions is not None:
        duration = transactions
        duration_unit = DurationUnit.TXN
        duration_desc = "trnsactions per job"
    elif workload_duration is not None:
        duration = workload_duration
        duration_unit = DurationUnit.SECOND
        duration_desc = "seconds"
    else:
        # Эта ветка теоретически не достижима благодаря MutuallyExclusiveOptionGroup,
        print("Error: None of the test duration parameters were specified!")
        click.get_current_context().exit(1)

    click.echo(
        f"Running workload with prefix_path={runner.table_folder}, scale={scale}, jobs={jobs}, client={processes}, mode={mode}, duration={duration} ({duration_desc})"
    )

    if processes == 1:
        # Single process execution
        metrics = runner.run(workload_start_time, duration, duration_unit, 0, jobs, single_session, script_selector, no_validate_scale)
    else:
        # Multi-process execution
        parallel_runner = ParallelRunner(runner)
        metrics = parallel_runner.run_parallel(
            workload_start_time, duration, duration_unit, processes, jobs, single_session, script_selector, no_validate_scale
        )

    # Print metrics summary
    metrics.print_summary()

    click.echo("Workload completed")


if __name__ == "__main__":
    cli()
