"""The FIX session is the user stream: execution reports, cancel rejects and session events flow
from the gateway's queue into Hummingbot's user-stream queue. Starting the stream starts the
gateway (with its reconnect loop); stopping it stops the gateway."""

import asyncio
from typing import Optional

from hummingbot.connector.exchange.bitfinex.bitfinex_fix_gateway import BitfinexFixGateway
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.logger import HummingbotLogger


class BitfinexFixUserStreamDataSource(UserStreamTrackerDataSource):
    _logger: Optional[HummingbotLogger] = None

    def __init__(self, gateway: BitfinexFixGateway):
        super().__init__()
        self._gateway = gateway

    @property
    def last_recv_time(self) -> float:
        return self._gateway.last_recv_time

    async def listen_for_user_stream(self, output: asyncio.Queue):
        if not self._gateway._running:
            await self._gateway.start()
        while True:
            output.put_nowait(await self._gateway.events.get())

    async def stop(self):
        await self._gateway.stop()
