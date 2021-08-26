import click

from .utils import get_latest_file_list_url, get_reversion_from_url


@click.command()
def main():
    """
    wizdiff
    """
    latest_file_url = get_latest_file_list_url()
    click.echo(f"{latest_file_url=}")
    reversion = get_reversion_from_url(latest_file_url)
    click.echo(f"{reversion=}")


if __name__ == "__main__":
    main()
