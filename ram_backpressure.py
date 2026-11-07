"""Bounded RAM ingress. Spoiler/text never wait behind queued media.

There is deliberately no task per received message and no unbounded backlog.
Overload policy: reject lower-priority incoming work, or replace the oldest
work of the same/worse priority; always call task_done for a displaced item.
"""
from __future__ import annotations

import asyncio
import heapq
import itertools
import logging

from config import Config

_log = logging.getLogger("autobot.ingress_queue")


def priority(env) -> tuple[int, int]:
    # Class priority always wins: a fast text/spoiler is never blocked by OCR.
    # Within each class, lower CHANNEL_CONFIG.priority wins (XX8=5 before LLwin=41).
    cid = int(getattr(env, 'chat_id', 0) or 0)
    site = getattr(Config, 'CHANNEL_CONFIG', {}).get(cid, {})
    try:
        site_priority = int(site.get('priority', 999))
    except (TypeError, ValueError):
        site_priority = 999
    return (message_tier(env), site_priority)


def message_tier(env) -> int:
    return content_tier(getattr(env, "message", None))


def content_tier(message) -> int:
    if message is None:
        return 4
    entities = getattr(message, "entities", None) or ()
    # SPOILER_FAST_PATH=false: spoiler xếp như text thường (tier 1).
    if getattr(Config, "SPOILER_FAST_PATH", True) and any(
            type(e).__name__ == "MessageEntitySpoiler" for e in entities):
        return 0
    text = str(getattr(message, "message", None) or getattr(message, "text", None) or "").strip()
    if text:
        return 1
    media = getattr(message, "media", None)
    if media is None:
        return 1
    document = getattr(media, "document", None) or getattr(message, "document", None)
    mime = str(getattr(document, "mime_type", "") or "").lower()
    if getattr(message, "photo", None) is not None or mime.startswith("image/"):
        return 2
    return 3


class PrioritizedIngressQueue(asyncio.Queue):
    """Maximum memory-bound queue with priority and responsible task accounting."""

    def __init__(self, maxsize: int = 128):
        super().__init__(maxsize=max(1, int(maxsize)))
        self.dropped_count = 0
        self.rejected_count = 0
        self._on_drop = None

    def set_drop_callback(self, callback):
        self._on_drop = callback

    def _evict(self, index):
        item = self._queue[index]
        del self._queue[index]
        self.task_done()
        self.dropped_count += 1
        if self._on_drop is not None:
            try:
                self._on_drop(item)
            except Exception:
                # A diagnostic/cleanup callback must never discard an incoming
                # Telegram event or corrupt asyncio.Queue's task accounting.
                _log.exception("[IngressQueue] drop callback failed")
        return item

    def drop_oldest(self):
        if not self._queue:
            return None
        worst = max(priority(item) for item in self._queue)
        index = next(i for i, item in enumerate(self._queue) if priority(item) == worst)
        return self._evict(index)

    def put_nowait(self, item):
        if self.full():
            worst = max(priority(old) for old in self._queue)
            if priority(item) > worst:
                self.rejected_count += 1
                raise asyncio.QueueFull
            self.drop_oldest()
        return super().put_nowait(item)

    async def put(self, item):
        return self.put_nowait(item)

    def _get(self):
        best = min(priority(item) for item in self._queue)
        index = next(i for i, item in enumerate(self._queue) if priority(item) == best)
        item = self._queue[index]
        del self._queue[index]
        return item


class PrioritizedOcrQueue(asyncio.Queue):
    """Hàng OCR có giới hạn: kênh priority nhỏ chạy trước (XX8=5, XX88=6, LLwin=41).

    Job là tuple (event, cfg, systems, account, queued_at); priority lấy từ
    cfg["priority"]. Cùng priority thì vào trước ra trước. Khi đầy: bỏ job cũ
    nhất trong nhóm priority KÉM nhất; job mới kém hơn mọi job đang chờ thì bị
    từ chối (QueueFull) thay vì chen mất job ưu tiên cao.
    """

    def __init__(self, maxsize: int = 12):
        super().__init__(maxsize=max(1, int(maxsize)))
        self._seq = itertools.count()
        self.dropped_count = 0
        self.rejected_count = 0

    @staticmethod
    def job_priority(job) -> int:
        try:
            return int((job[1] or {}).get("priority", 999))
        except (TypeError, ValueError, IndexError, AttributeError):
            return 999

    def _init(self, maxsize):
        self._queue = []

    def _put(self, item):
        heapq.heappush(self._queue, (self.job_priority(item), next(self._seq), item))

    def _get(self):
        return heapq.heappop(self._queue)[2]

    def _evict_worst(self):
        index = max(range(len(self._queue)),
                    key=lambda i: (self._queue[i][0], -self._queue[i][1]))
        entry = self._queue.pop(index)
        heapq.heapify(self._queue)
        self.task_done()
        self.dropped_count += 1
        return entry[2]

    def drop_oldest(self):
        return self._evict_worst() if self._queue else None

    def put_nowait(self, item):
        if self.full():
            if self.job_priority(item) > max(entry[0] for entry in self._queue):
                self.rejected_count += 1
                raise asyncio.QueueFull
            self._evict_worst()
        return super().put_nowait(item)

    async def put(self, item):
        return self.put_nowait(item)
