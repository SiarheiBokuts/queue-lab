import json

from fastapi import WebSocket


class Hub:
    """Single live channel: broadcasts JSON messages to every connected browser."""

    def __init__(self):
        self.clients: set[WebSocket] = set()

    async def add(self, ws: WebSocket):
        await ws.accept()
        self.clients.add(ws)

    def remove(self, ws: WebSocket):
        self.clients.discard(ws)

    async def broadcast(self, obj: dict):
        if not self.clients:
            return
        data = json.dumps(obj)
        for ws in list(self.clients):
            try:
                await ws.send_text(data)
            except Exception:  # noqa: BLE001
                self.clients.discard(ws)
