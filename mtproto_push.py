"""MTProto Push Socket bridge: Telethon's persistent TCP -> bounded RAM.

There is no WebSocket, HTTP polling, GetHistory, OCR, or browser work in event
handlers. Optional bounded GetChannelDifference runs outside the handlers.
Handlers register BEFORE TelegramClient.start()/connect().
"""
from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from typing import Callable, Iterable
from types import SimpleNamespace

from telethon.utils import get_peer_id
from mtproto_channel_diff import ChannelDifferenceSupervisor

from telethon import events

from tg_ingest import OfferResult, ReceiverConfig, TelegramReceiver


class MTProtoPushSocket:
    def __init__(self, client, chats: Iterable[int], *, queue, started_at: datetime,
                 config: ReceiverConfig, logger, on_accepted: Callable | None = None,
                 on_raw_update: Callable | None = None, diagnostics=False,
                 channel_config=None, difference_config=None):
        self.client = client
        self.queue = queue
        self.chats = set(map(int, chats))
        self.log = logger
        self.on_accepted = on_accepted
        self.on_raw_update = on_raw_update
        self.diagnostics = diagnostics
        settings = difference_config or {}
        self.difference = ChannelDifferenceSupervisor(
            client, channel_config or {cid: {} for cid in self.chats},
            offer=self.receiver_offer, log=logger, **settings,
        )
        self._started_at = started_at
        self._registered = False
        self._last_overload_log = 0.0
        self.receiver = TelegramReceiver(
            client, self.chats, self.queue.put_nowait,
            accept=self._accept_live_message,
            cfg=config, log=logger,
        )

    def _accept_live_message(self, message):
        # Suppress older updates delivered during initial state sync/reconnect.
        # This is intentionally *not* polling; just checking the message date.
        date = getattr(message, 'edit_date', None) or getattr(message, 'date', None)
        if date is None:
            return False
        if date.tzinfo is None:
            date = date.replace(tzinfo=timezone.utc)
        return date >= self._started_at - timedelta(seconds=15)

    def receiver_offer(self, chat_id, message, *, edited=False, source='channel_difference'):
        # Reconciled updates must share the same admission counters/log throttling
        # as socket updates, not bypass the bookkeeping.
        return self._on_event(SimpleNamespace(chat_id=chat_id, message=message),
                              edited, source=source)

    def _on_event(self, event, edited, *, source='mtproto_push'):
        result = self.receiver.offer_nowait(getattr(event, 'chat_id', None),
                                            getattr(event, 'message', None), edited=edited, source=source)
        if result == OfferResult.ACCEPTED:
            if self.on_accepted is not None:
                try:
                    self.on_accepted(event, edited)
                except Exception:
                    # Statistics must not stall or abort the MTProto callback.
                    self.log.exception('[MTProtoPush] accepted statistics hook failed')
        elif result == OfferResult.SINK_FAILED:
            now = time.monotonic()
            if now - self._last_overload_log >= 10.0:
                self._last_overload_log = now
                self.log.warning('[MTProtoPush] ingress overflow; queue=%s/%s dropped=%s rejected=%s',
                                 self.queue.qsize(), self.queue.maxsize,
                                 self.queue.dropped_count, self.queue.rejected_count)
        return result

    async def _new_message(self, event):
        # Deliberately no await: transport callback never waits for OCR/browser.
        self._on_event(event, False)

    async def _edited_message(self, event):
        self._on_event(event, True)

    async def _raw_update(self, event):
        # Raw transport notifications are a second admission source. They are
        # deduplicated with NewMessage/MessageEdited and never issue an RPC.
        kind = type(event).__name__
        if kind in ('UpdateNewChannelMessage', 'UpdateEditChannelMessage'):
            msg = getattr(event, 'message', None)
            peer = getattr(msg, 'peer_id', None)
            try:
                chat_id = int(get_peer_id(peer))
            except (ValueError, TypeError):
                chat_id = None
            if chat_id in self.chats and msg is not None:
                state = self.difference.states.get(chat_id)
                if state is not None:
                    msg = self.difference._recovered_message(msg, SimpleNamespace(users=(), chats=()), state)
                self._on_event(SimpleNamespace(chat_id=chat_id, message=msg),
                               kind == 'UpdateEditChannelMessage', source='mtproto_raw')
        if self.on_raw_update is not None:
            self.on_raw_update(event)

    def register(self):
        if self._registered:
            return self
        # Filtering only by numeric ID in RAM, not via Telethon chats= which
        # might wait for entity resolution RPCs before handlers are ready.
        self.client.add_event_handler(self._new_message, events.NewMessage())
        self.client.add_event_handler(self._edited_message, events.MessageEdited())
        self.client.add_event_handler(self._raw_update, events.Raw())
        self._registered = True
        self.log.info('[MTProtoPush] handlers registered before socket connect: channels=%s', len(self.chats))
        return self

    async def prewarm(self, *, limit, timeout, config):
        # One bounded dialog sync during startup; keep PTS/access_hash in RAM.
        dialogs = await self.client.get_dialogs(limit=int(limit))
        hits = self.difference.cache_dialogs(dialogs)
        await self.difference.fill_missing(config, timeout=timeout)
        self.log.info('[MTProtoPush] prewarm dialogs=%s cached=%s peers=%s pts=%s',
                      len(dialogs), hits,
                      self.difference.stats()['cached_peers'], self.difference.stats()['cached_pts'])
        return self.difference.stats()

    def start_active_difference(self, allowed_chat_ids=None):
        if allowed_chat_ids is not None:
            self.difference.restrict(allowed_chat_ids)
        return self.difference.start()

    async def stop(self):
        await self.difference.stop()

    def stats(self):
        return {**self.receiver.stats(), 'ingress_size': self.queue.qsize(),
                'ingress_capacity': self.queue.maxsize,
                'overload_evicted': self.queue.dropped_count,
                'overload_rejected': self.queue.rejected_count, 'difference': self.difference.stats()}
