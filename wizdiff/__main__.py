import click
from pathlib import Path

from .update_notifier import WebhookUpdateNotifier


@click.command()
@click.option("--sleep-time", type=int, default=3_600)
@click.option("--webhook")
@click.option("--thread")
def main(sleep_time, webhook, thread):
    """
    wizdiff
    """
    update_handler = WebhookUpdateNotifier(webhook, thread, sleep_time=sleep_time)

    if not Path("wizdiff.db").exists():
        # add initial data to compare here
        click.echo("No database found creating a new one")
        update_handler.init_db()

    update_handler.update_loop()


if __name__ == "__main__":
    main()
