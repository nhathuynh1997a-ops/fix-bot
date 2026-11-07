"""Optional, bounded MTProto channel-difference reconciliation.

Telethon remains the owner of the update stream. This sidecar keeps its OWN
per-channel PTS, never changes Telethon's internal message box/session state,
and gives recovered live messages to the same ingress/deduplication path.

Official API warns against continuously polling idle channels. Enable only
selected urgent channels, keep the interval conservative, and obey FloodWait.
"""
from __future__ import annotations

import asyncio
import logging
import random
import time
from dataclasses import dataclass
from typing import Callable

from telethon import functions, types, utils
from telethon.errors import FloodWaitError

_LOG = logging.getLogger(__name__)


@dataclass(slots=True)
class ChannelState:
    chat_id: int
    priority: int
    input_peer: object | None = None
    pts: int = 0
    source: str = "missing"
    failures: int = 0
    next_delay: float = 0.0

    @property
    def ready(self) -> bool:
        return (self.input_peer is not None and self.pts > 0)


class ChannelDifferenceSupervisor:
    def __init__(self, client, channels: dict, *, offer: Callable, log=None,
                 timeout=10.0, interval=6.0, priority_max=6,
                 concurrency=1, enabled=True, chat_ids=(), max_channels=10):
        self.client = client
        self.offer = offer
        self.log = log or _LOG
        self.timeout = max(1.0, min(10.0, float(timeout)))
        self.interval = max(5.0, float(interval))
        self.priority_max = int(priority_max)
        self.sem = asyncio.Semaphore(max(1, min(2, int(concurrency))))
        self.enabled = bool(enabled)
        self.chat_ids = set(map(int, chat_ids))
        self.max_channels = max(1, min(10, int(max_channels)))
        self._flood_until = 0.0
        self._next_request_at = 0.0
        self._rpc_lock = asyncio.Lock()
        self.states: dict[int, ChannelState] = {
            int(cid): ChannelState(int(cid), int(cfg.get('priority', 999)))
            for cid, cfg in channels.items() if cfg.get('enabled', True)
        }
        self.tasks: list[asyncio.Task] = []
        self.metrics = dict(requests=0, recovered=0, empty=0, errors=0,
                            floodwait=0, too_long=0)
        self._page_burst: dict[int, int] = {}

    @staticmethod
    def _peer_from_entity(entity):
        if entity is None:
            return None
        # Telethon Dialog.input_entity is InputPeerChannel, with access_hash.
        if isinstance(entity, types.InputPeerChannel):
            return entity
        try:
            result = utils.get_input_peer(entity)
            return result if isinstance(result, types.InputPeerChannel) else None
        except (TypeError, ValueError):
            return None

    def cache_dialogs(self, dialogs):
        hits = 0
        for d in dialogs:
            raw_entity = getattr(d, 'entity', None)
            if raw_entity is None:
                continue
            try:
                cid = int(utils.get_peer_id(raw_entity))
            except (TypeError, ValueError):
                continue
            state = self.states.get(cid)
            if state is None:
                continue
            peer = self._peer_from_entity(getattr(d, 'input_entity', None) or raw_entity)
            if peer is None:
                continue
            state.input_peer = peer
            # dialogs.Dialog.pts is the channel's initial update state.
            pts = int(getattr(getattr(d, 'dialog', None), 'pts', 0) or 0)
            if pts > 0:
                state.pts = max(state.pts, pts)
            state.source = 'dialog'
            hits += 1
        return hits

    async def fill_missing(self, config: dict, *, timeout: float):
        """Best-effort peer/PTS hydration with a single startup time budget."""
        deadline = time.monotonic() + max(1.0, timeout)
        throttle = asyncio.Semaphore(2)
        stop_on_flood = asyncio.Event()

        async def fill(state):
            async with throttle:
                if stop_on_flood.is_set():
                    return
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return
                cfg = config.get(state.chat_id, {})
                try:
                    if state.input_peer is None:
                        name = cfg.get('username') or state.chat_id
                        obj = await asyncio.wait_for(self.client.get_input_entity(name),
                                                     min(self.timeout, remaining))
                        state.input_peer = self._peer_from_entity(obj)
                        if state.input_peer is not None:
                            state.source = 'entity'
                    if state.input_peer is None or state.pts > 0:
                        return
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        return
                    peer = state.input_peer
                    full = await asyncio.wait_for(self.client(
                        functions.channels.GetFullChannelRequest(
                            types.InputChannel(peer.channel_id, peer.access_hash))),
                        min(self.timeout, remaining))
                    initial = int(getattr(getattr(full, 'full_chat', None), 'pts', 0) or 0)
                    if initial > 0:
                        state.pts = initial
                        state.source = 'full_channel'
                except asyncio.CancelledError:
                    raise
                except FloodWaitError as exc:
                    stop_on_flood.set()
                    self.metrics['floodwait'] += 1
                    self.log.warning('[MTProtoDiff] startup FloodWait %ss, stop resolving more RPCs', exc.seconds)
                    # Do not sleep inside startup: missing states remain push-only.
                except Exception as exc:
                    self.log.debug('[MTProtoDiff] could not hydrate chat=%s: %s', state.chat_id, exc)

        pending = [s for s in self.states.values() if not s.ready]
        if pending:
            await asyncio.wait_for(asyncio.gather(*(fill(s) for s in pending)),
                                   timeout=max(1.0, timeout))
        return sum(st.ready for st in self.states.values())

    def _input_channel(self, state):
        peer = state.input_peer
        return types.InputChannel(peer.channel_id, peer.access_hash)

    def _recovered_message(self, message, response, state):
        """Use Telethon's custom Message wrapper when available for media/OCR."""
        try:
            entities = {utils.get_peer_id(x): x for x in
                        (*getattr(response, 'users', ()), *getattr(response, 'chats', ())) }
            # Telethon 1.35 already patches TL Message with the media helpers.
            # It has _finish_init, not CustomMessage._new. Hydrate its client
            # and input chat so recovered photo/video downloads work too.
            message._finish_init(self.client, entities, state.input_peer)
        except Exception:
            self.log.debug('[MTProtoDiff] message initialization unavailable', exc_info=True)
        return message

    async def _one_request(self, state):
        request = functions.updates.GetChannelDifferenceRequest(
            force=False, channel=self._input_channel(state),
            filter=types.ChannelMessagesFilterEmpty(),
            pts=state.pts, limit=100)
        async with self.sem:
            # FloodWait applies to the shared account/method, not just one
            # channel. Serialize admission and space RPCs across all watchers.
            async with self._rpc_lock:
                while True:
                    delay = max(self._flood_until, self._next_request_at) - time.monotonic()
                    if delay <= 0:
                        break
                    await asyncio.sleep(delay)
                self._next_request_at = time.monotonic() + .5
            try:
                # Telethon 1.35's public __call__ drops the per-request flood
                # threshold argument. Its _call supports it: keep server waits
                # out of the global dispatcher and handle them here instead.
                low_call = getattr(self.client, '_call', None)
                sender = getattr(self.client, '_sender', None)
                operation = (low_call(sender, request, flood_sleep_threshold=0)
                             if low_call is not None and sender is not None
                             else self.client(request))
                answer = await asyncio.wait_for(operation, self.timeout)
            except FloodWaitError as exc:
                self._flood_until = max(self._flood_until, time.monotonic() + exc.seconds + 2)
                raise
        self.metrics['requests'] += 1
        state.next_delay = max(self.interval, float(getattr(answer, 'timeout', 0) or 0))
        name = type(answer).__name__
        if name == 'ChannelDifferenceTooLong':
            self.metrics['too_long'] += 1
            fresh_pts = int(getattr(getattr(answer, 'dialog', None), 'pts', 0) or 0)
            if fresh_pts > state.pts:
                state.pts = fresh_pts
            else:
                # No safe restart position. Leave this channel push-only rather
                # than repeatedly requesting a stale PTS and triggering floods.
                state.pts = 0
            self.log.warning('[MTProtoDiff] too-long difference chat=%s, live-only, pts=%s',
                             state.chat_id, state.pts)
            return True
        if name == 'ChannelDifferenceEmpty':
            self.metrics['empty'] += 1
        else:
            for msg in getattr(answer, 'new_messages', ()):
                if getattr(msg, 'id', None) is not None:
                    self.offer(state.chat_id, self._recovered_message(msg, answer, state),
                               edited=False, source='channel_difference')
                    self.metrics['recovered'] += 1
            for update in getattr(answer, 'other_updates', ()):
                update_type = type(update).__name__
                if update_type in ('UpdateNewChannelMessage', 'UpdateEditChannelMessage'):
                    msg = getattr(update, 'message', None)
                    if getattr(msg, 'id', None) is not None:
                        self.offer(state.chat_id, self._recovered_message(msg, answer, state),
                                   edited=update_type == 'UpdateEditChannelMessage',
                                   source='channel_difference')
                        self.metrics['recovered'] += 1
        next_pts = int(getattr(answer, 'pts', 0) or 0)
        if next_pts > state.pts:
            state.pts = next_pts
        return bool(getattr(answer, 'final', True))

    async def _watch_channel(self, state):
        # Spread RPCs on startup to avoid a burst of requests.
        await asyncio.sleep(random.uniform(0.2, min(2.0, self.interval)))
        backoff = 0.0
        while True:
            try:
                if not state.ready:
                    self.log.warning('[MTProtoDiff] disabled channel=%s: valid pts/access_hash unavailable',
                                     state.chat_id)
                    return
                if not self.client.is_connected():
                    await asyncio.sleep(self.interval)
                    continue
                final = await self._one_request(state)
                state.failures = 0
                backoff = 0.0
                # Paginate at most 3 pages before a cooldown; avoid starving
                # the socket and triggering floods during large bursts.
                if not final:
                    pages = self._page_burst.get(state.chat_id, 0) + 1
                    self._page_burst[state.chat_id] = pages
                    if pages < 3:
                        await asyncio.sleep(0.5)
                        continue
                self._page_burst[state.chat_id] = 0
                await asyncio.sleep(max(self.interval, state.next_delay) + random.uniform(0.0, 0.5))
            except asyncio.CancelledError:
                raise
            except FloodWaitError as exc:
                self.metrics['floodwait'] += 1
                backoff = max(self.interval, float(exc.seconds) + 2.0)
                self.log.warning('[MTProtoDiff] FloodWait chat=%s sleeping=%.0fs', state.chat_id, backoff)
                await asyncio.sleep(backoff)
            except Exception as exc:
                state.failures += 1
                self.metrics['errors'] += 1
                if state.failures >= 10:
                    self.log.error('[MTProtoDiff] giving up chat=%s after 10 failures, push remains live',
                                   state.chat_id)
                    return
                backoff = min(300.0, self.interval * 2 ** min(state.failures, 6))
                if state.failures == 1 or state.failures % 5 == 0:
                    self.log.warning('[MTProtoDiff] request failed chat=%s retry=%.1fs (%s)',
                                     state.chat_id, backoff, type(exc).__name__)
                await asyncio.sleep(backoff)

    def restrict(self, allowed_chat_ids):
        """After membership checks, never poll channels removed from config."""
        allowed = set(map(int, allowed_chat_ids))
        if self.tasks:
            raise RuntimeError('Cannot restrict active difference watcher')
        self.states = {cid: state for cid, state in self.states.items() if cid in allowed}

    def start(self):
        if self.tasks or not self.enabled:
            return 0
        selected = sorted((s for s in self.states.values()
                           if (s.chat_id in self.chat_ids if self.chat_ids
                               else s.priority <= self.priority_max) and s.ready),
                          key=lambda s: (s.priority, s.chat_id))[:self.max_channels]
        for state in selected:
            self.tasks.append(asyncio.create_task(
                self._watch_channel(state), name=f'mtproto-diff-{state.chat_id}'))
        self.log.info('[MTProtoDiff] active=%s ready=%s/%s threshold=%s timeout=%.1fs interval=%.1fs',
                      len(selected), sum(s.ready for s in self.states.values()),
                      len(self.states), self.priority_max, self.timeout, self.interval)
        self.log.info('[MTProtoDiff] selected_chats=%s', [s.chat_id for s in selected])
        return len(selected)

    async def stop(self):
        for task in self.tasks:
            task.cancel()
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)
        self.tasks.clear()

    def stats(self):
        return {**self.metrics, 'cached_peers': sum(s.input_peer is not None for s in self.states.values()),
                'cached_pts': sum(s.pts > 0 for s in self.states.values()),
                'active_tasks': sum(not task.done() for task in self.tasks)}
