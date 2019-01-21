import click

from ..clients import base_client


def _compose_url(scheme, username, password, hostname, port, path, query):
    url = base_client.URL.from_parts(
        scheme=scheme,
        username=username,
        password=username,
        hostname=hostname,
        port=port,
        path=path,
        query=query,
    ).url
    return url


@click.command()
@click.option("--scheme", default=None, required=True)
@click.option("--username", default=None)
@click.option("--password", default=None)
@click.option("--hostname", default=None)
@click.option("--port", default=None)
@click.option("--path", default=None)
@click.option("--query", default=None)
def command_compose_url(scheme, username, password, hostname, port, path, query):
    url = _compose_url(
        scheme=scheme,
        username=username,
        password=username,
        hostname=hostname,
        port=port,
        path=path,
        query=query,
    )
    click.echo(url, nl=False)
