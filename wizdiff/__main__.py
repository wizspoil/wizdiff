import sys
from pathlib import Path

import click
from loguru import logger

from .update_notifier import UpdateNotifier, WebhookUpdateNotifier


logger.enable("wizdiff")
logger.remove(0)
logger.add(sys.stdout, enqueue=True)


@click.command()
@click.option("--sleep-time", type=int, default=3_600)
@click.option("--webhook")
@click.option("--thread")
def main(sleep_time, webhook, thread):
    """
    wizdiff
    """
    update_handler = WebhookUpdateNotifier([webhook], thread, sleep_time=sleep_time)

    if not Path("wizdiff.db").exists():
        # add initial data to compare
        click.echo("No database found creating a new one")
        update_handler.init_db()

    update_handler.update_loop()


if __name__ == "__main__":
    main()
