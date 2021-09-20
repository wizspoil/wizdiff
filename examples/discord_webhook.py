from typing import List, Tuple

from wizdiff import ChangedFileDelta, CreatedFileDelta, DeletedFileDelta, FileDelta, UpdateNotifier


class WebhookUpdateNotifier(UpdateNotifier):
    def __init__(self, webhook_urls: List[str], thread_id: int = None, **kwargs):
        super().__init__(**kwargs)
        self.webhook_urls = webhook_urls
        self.thread_id = thread_id

    def send_to_webhook(
            self,
            webhook_url,
            content: str,
            *,
            file: Tuple[str, bytes] = None,
            params: dict = None,
            **json_fields,
    ):
        """
        file is a tuple of file name and file data
        """
        to_send = {
            "content": content,
        }
        to_send.update(json_fields)

        if not to_send.get("username"):
            to_send["username"] = "wizdiff"

        files = None

        if file:
            files = [file]

        if self.thread_id:
            if not params:
                params = {}

            params["thread_id"] = self.thread_id

        await self.webdriver.session.post(
            webhook_url,
            json=to_send,
            files=files,
            params=params,
        )

    def send_to_all_webhooks(self, *args, **kwargs):
        for webhook_url in self.webhook_urls:
            self.send_to_webhook(webhook_url, *args, **kwargs)

    def notify_revision_update(self, revision: str):
        self.send_to_all_webhooks(f"New revision {revision} found")

    def notify_non_wad_file_update(self, delta: FileDelta):
        # TODO: 3.10 switch to match
        if (delta_type := type(delta)) is CreatedFileDelta:
            delta: CreatedFileDelta
            self.send_to_all_webhooks(
                f"New file {delta.name} found; download at: {delta.url}"
            )

        elif delta_type is ChangedFileDelta:
            delta: ChangedFileDelta
            message = f"Changed file {delta.name} found; "

            if delta.old_size > delta.new_size:
                message += f"{delta.old_size - delta.new_size} bytes smaller; "

            elif delta.old_size < delta.new_size:
                message += f"{delta.new_size - delta.old_size} bytes larger; "

            # crc change
            else:
                message += "size unchanged (new crc); "

            message += f"download at {delta.url}"

            self.send_to_all_webhooks(message)

        elif delta_type is DeletedFileDelta:
            delta: DeletedFileDelta
            self.send_to_all_webhooks(f"File {delta.name} was deleted")

        else:
            raise RuntimeError(f"Unhandled delta type {delta_type}")

    def notify_wad_file_update(self, delta: FileDelta):
        self.send_to_all_webhooks(str(delta))
