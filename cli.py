#!/usr/bin/env python3
import click
import os
import logging
from multiprocessing import Pool
from runner import Runner

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


def create_runner_from_config(endpoint, database, cert_file, user, password):
    """
    Create a Runner instance from configuration.
    
    Args:
        endpoint: YDB endpoint
        database: Database path
        cert_file: Path to certificate file
        user: Username
        password: Password
        
    Returns:
        Runner instance
    """
    return Runner(
        endpoint=endpoint,
        database=database,
        root_certificates_file=cert_file,
        user=user,
        password=password
    )


def get_config_value(cli_value, env_var, required=False):
    """
    Get configuration value from CLI option or environment variable.
    
    Args:
        cli_value: Value from CLI option
        env_var: Environment variable name
        required: Whether the value is required
        
    Returns:
        Configuration value
        
    Raises:
        click.ClickException: If required value is missing
    """
    value = cli_value or os.getenv(env_var)
    if required and not value:
        raise click.ClickException(f"Missing required parameter. Provide --{env_var.lower().replace('_', '-')} or set {env_var} environment variable")
    return value


@click.group()
def cli():
    """YDB pgbench-like workload tool."""
    pass


@cli.command()
@click.option('--endpoint', help='YDB endpoint (e.g., grpcs://host:2135)')
@click.option('--database', help='Database path (e.g., /Root/database)')
@click.option('--cert-file', help='Path to root certificate file')
@click.option('--user', help='Username for authentication')
@click.option('--password', help='Password for authentication')
@click.option('--scale', default=100, help='Number of branches to create (default: 100)')
@click.option('--processes', default=1, help='Number of parallel processes (default: 1)')
def init(endpoint, database, cert_file, user, password, scale, processes):
    """Initialize database tables with test data."""
    # Get configuration from CLI options or environment variables
    endpoint = get_config_value(endpoint, 'YDB_ENDPOINT', required=True)
    database = get_config_value(database, 'YDB_DATABASE', required=True)
    cert_file = get_config_value(cert_file, 'YDB_ROOT_CERT')
    user = get_config_value(user, 'YDB_USER')
    password = get_config_value(password, 'YDB_PASSWORD')
    
    click.echo(f"Initializing database with scale={scale}, processes={processes}")
    
    def init_worker(process_id):
        """Worker function for multiprocessing."""
        if processes > 1:
            click.echo(f"Process {process_id} started")
        runner = create_runner_from_config(endpoint, database, cert_file, user, password)
        runner.init_tables(scale)
    
    if processes == 1:
        # Single process execution
        init_worker(0)
    else:
        # Multi-process execution
        with Pool(processes) as pool:
            pool.map(init_worker, range(processes))
    
    click.echo("Initialization completed")


@cli.command()
@click.option('--endpoint', help='YDB endpoint (e.g., grpcs://host:2135)')
@click.option('--database', help='Database path (e.g., /Root/database)')
@click.option('--cert-file', help='Path to root certificate file')
@click.option('--user', help='Username for authentication')
@click.option('--password', help='Password for authentication')
@click.option('--scale', default=100, help='Number of branches (must match init scale, default: 100)')
@click.option('--workers', default=7, help='Number of async workers per process (default: 7)')
@click.option('--transactions', default=100, help='Number of transactions per worker (default: 100)')
@click.option('--processes', default=1, help='Number of parallel processes (default: 1)')
@click.option('--single-session', is_flag=True, help='Use single session mode instead of pooled mode')
def run(endpoint, database, cert_file, user, password, scale, workers, transactions, processes, single_session):
    """Run workload against the database."""
    # Get configuration from CLI options or environment variables
    endpoint = get_config_value(endpoint, 'YDB_ENDPOINT', required=True)
    database = get_config_value(database, 'YDB_DATABASE', required=True)
    cert_file = get_config_value(cert_file, 'YDB_ROOT_CERT')
    user = get_config_value(user, 'YDB_USER')
    password = get_config_value(password, 'YDB_PASSWORD')
    
    mode = "single session" if single_session else "pooled"
    click.echo(f"Running workload with scale={scale}, workers={workers}, transactions={transactions}, processes={processes}, mode={mode}")
    
    def run_worker(process_id):
        """Worker function for multiprocessing."""
        if processes > 1:
            click.echo(f"Process {process_id} started")
        runner = create_runner_from_config(endpoint, database, cert_file, user, password)
        runner.run(workers, transactions, scale, single_session)
    
    if processes == 1:
        # Single process execution
        run_worker(0)
    else:
        # Multi-process execution
        with Pool(processes) as pool:
            pool.map(run_worker, range(processes))
    
    click.echo("Workload completed")


if __name__ == '__main__':
    cli()