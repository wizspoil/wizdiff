import click
from pathlib import Path

from .update_notifier import UpdateNotifier


@click.command()
def main():
    """
    wizdiff
    """
    update_handler = UpdateNotifier()

    if not Path("wizdiff.db").exists():
        # add initial data to compare here
        click.echo("No database found creating a new one")
        update_handler.init_db()

    update_handler.update_loop()


if __name__ == "__main__":
    main()
