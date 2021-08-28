import click
from pathlib import Path

from .update_notifier import WebhookUpdateNotifier


@click.command()
@click.option("--webhook")
@click.option("--thread_id")
def main(webhook, thread_id):
    """
    wizdiff
    """
    update_handler = WebhookUpdateNotifier(webhook, thread_id)

    if not Path("wizdiff.db").exists():
        # add initial data to compare here
        click.echo("No database found creating a new one")
        update_handler.init_db()

    update_handler.update_loop()


if __name__ == "__main__":
    main()
