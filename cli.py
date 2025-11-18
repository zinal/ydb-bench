#!/usr/bin/env python3
import click
import logging
import re
from multiprocessing import Pool
from runner import Runner

# Configure logging to stderr
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=__import__('sys').stderr
)


def create_runner_from_config(endpoint, database, cert_file, user, password, table_folder):
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
        table_folder=table_folder
    )




def validate_table_folder(_ctx, _param, table_folder: str) -> str:
    """
    Validate and sanitize table folder name to prevent SQL injection.
    """
    if not re.match(r'^[a-zA-Z0-9_\-\/]+$', table_folder):
        raise click.ClickException(
            f"Invalid table folder name '{table_folder}'. "
            "Only alphanumeric characters, underscores, hyphens and backslashes are allowed."
        )
    return table_folder


@click.group()
@click.option('--endpoint', '-e', envvar='YDB_ENDPOINT', required=True, help='Endpoint to connect. (e.g., grpcs://host:2135)')
@click.option('--database', '-d', envvar='YDB_DATABASE', required=True, help='Database to work with (e.g., /Root/database)')
@click.option('--ca-file', envvar='YDB_ROOT_CERT', help='Path to root certificate file')
@click.option('--user', envvar='YDB_USER', help='Username for authentication')
@click.option('--password', envvar='YDB_PASSWORD', help='Password for authentication')
@click.option('--pefix-path', envvar='YDB_PREFIX_PATH', default='pgbench', callback=validate_table_folder, help='Folder name for tables (default: pgbench)')
@click.option('--scale', '-s', type=int, default=100, help='Number of branches to create (default: 100)')
@click.option('--processes', type=int, default=1, help='Number of parallel processes (default: 1)')
@click.pass_context
def cli(ctx, endpoint, database, ca_file, user, password, table_folder, scale, processes):
    """YDB pgbench-like workload tool."""
    
    # Store common configuration in context
    ctx.ensure_object(dict)
    ctx.obj['endpoint'] = endpoint
    ctx.obj['database'] = database
    ctx.obj['ca_file'] = ca_file
    ctx.obj['user'] = user
    ctx.obj['password'] = password
    ctx.obj['table_folder'] = table_folder
    ctx.obj['scale'] = scale
    ctx.obj['processes'] = processes


@cli.command()
@click.pass_context
def init(ctx):
    """Initialize database tables with test data."""
    # Get common configuration from context
    endpoint = ctx.obj['endpoint']
    database = ctx.obj['database']
    ca_file = ctx.obj['ca_file']
    user = ctx.obj['user']
    password = ctx.obj['password']
    table_folder = ctx.obj['table_folder']
    scale = ctx.obj['scale']
    processes = ctx.obj['processes']
    
    click.echo(f"Initializing database with table_folder={table_folder}, scale={scale}, processes={processes}")
    
    def init_job(process_id):
        """Job function for multiprocessing."""
        if processes > 1:
            click.echo(f"Process {process_id} started")
        runner = create_runner_from_config(endpoint, database, ca_file, user, password, table_folder)
        runner.init_tables(scale)
    
    if processes == 1:
        # Single process execution
        init_job(0)
    else:
        # Multi-process execution
        with Pool(processes) as pool:
            pool.map(init_job, range(processes))
    
    click.echo("Initialization completed")


@cli.command()
@click.option('--jobs', '-j', type=int, default=1, help='Number of async jobs per process (default: 1)')
@click.option('--transactions', '-t', type=int, default=100, help='Number of transactions each job runs (default: 100)')
@click.option('--single-session', is_flag=True, help='Use single session mode instead of pooled mode')
@click.option('--file', '-f', type=click.Path(exists=True, readable=True), help='Path to file containing SQL script to execute')
@click.pass_context
def run(ctx, jobs, transactions, single_session, file):
    """Run workload against the database."""
    # Get common configuration from context
    endpoint = ctx.obj['endpoint']
    database = ctx.obj['database']
    ca_file = ctx.obj['ca_file']
    user = ctx.obj['user']
    password = ctx.obj['password']
    table_folder = ctx.obj['table_folder']
    scale = ctx.obj['scale']
    processes = ctx.obj['processes']
    
    # Read script from file if provided
    script = None
    if file:
        with open(file, 'r') as f:
            script = f.read()
        click.echo(f"Using script from file: {file}")
    
    mode = "single session" if single_session else "pooled"
    click.echo(f"Running workload with table_folder={table_folder}, scale={scale}, jobs={jobs}, transactions={transactions}, processes={processes}, mode={mode}")
    
    def run_job(process_id):
        """Job function for multiprocessing."""
        if processes > 1:
            click.echo(f"Process {process_id} started")
        runner = create_runner_from_config(endpoint, database, ca_file, user, password, table_folder)
        runner.run(jobs, transactions, scale, single_session, script)
    
    if processes == 1:
        # Single process execution
        run_job(0)
    else:
        # Multi-process execution
        with Pool(processes) as pool:
            pool.map(run_job, range(processes))
    
    click.echo("Workload completed")


if __name__ == '__main__':
    cli()