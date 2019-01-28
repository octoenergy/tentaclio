import click

from ..clients import base_client


def _compose_url(scheme, username, password, hostname, port, path, query):
    url = base_client.URL.from_components(
        scheme=scheme,
        username=username,
        password=password,
        hostname=hostname,
        port=port,
        path=path,
        query=query
    ).url
    return url


@click.command()
@click.option('--scheme', default=None, required=True)
@click.option('--username', default=None)
@click.option('--password', default=None)
@click.option('--hostname', default=None)
@click.option('--port', default=None, type=int)
@click.option('--path', default=None)
@click.option('--key', default=None, nargs=2, multiple=True,
              help='Provide key-value pairs using [--key KEY VALUE]')
def command_compose_url(scheme, username, password, hostname, port, path, key):
    if not hostname and not path:
        raise click.ClickException('Provide at least one of `hostname`, `path`.')
    query = key
    if query:
        query = {k: v for k, v in key}
    else:
        query = None
    url = _compose_url(
        scheme=scheme,
        username=username,
        password=password,
        hostname=hostname,
        port=port,
        path=path,
        query=query
    )
    click.echo(url, nl=False)
