

import asyncio
import random
import time

from logger_setup import logger
from freshness import age_seconds

# PATCH_MARKER_2026-10-28-queue-bounded-priority

XX88_SPOILER_CHANNEL_ID = -1002817093108
LLWIN_SPOILER_CHANNEL_ID = -1003873843218


def is_priority_xx88_spoiler(item):
    if not isinstance(item, dict):
        return False
    try:
        chat_id = int(item.get('chat_id') or 0)
    except (TypeError, ValueError):
        return False
    return (
        (chat_id == XX88_SPOILER_CHANNEL_ID or (
            chat_id == LLWIN_SPOILER_CHANNEL_ID
            and str(item.get('domain') or '').lower() == 'llwincode.com'))
        and str(item.get('source_kind') or item.get('source') or '').lower() == 'spoiler'
    )


class XX88SpoilerPriorityQueue(asyncio.Queue):

    def __init__(self, maxsize=0):
        super().__init__(maxsize=maxsize)
        self._on_drop = None
        self.dropped_count = 0

    def set_drop_callback(self, callback):
        self._on_drop = callback

    def drop_oldest(self):
        if self.empty():
            return None
        item = self._queue.popleft()
        self.task_done()
        self.dropped_count += 1
        if self._on_drop is not None:
            try:
                self._on_drop(item)
            except Exception:
                logger.exception("[Queue] drop callback failed")
        logger.warning("[Queue] queue full, dropped oldest item: %s", _item_label(item))
        return item

    def put_nowait(self, item):
        if self.full():
            worst = max(_dispatch_priority(old) for old in self._queue)
            if _dispatch_priority(item) > worst:
                raise asyncio.QueueFull
            self.drop_oldest()
        super().put_nowait(item)

    async def put(self, item):
        self.put_nowait(item)

    def _put(self, item):
        if not is_priority_xx88_spoiler(item):
            return super()._put(item)

        insertion = 0
        for pending in self._queue:
            if not is_priority_xx88_spoiler(pending):
                break
            insertion += 1
        self._queue.insert(insertion, item)


def _item_label(item):
    if isinstance(item, dict):
        return item.get("code") or item.get("work_item_id") or item.get("inbox_id") or "unknown"
    return str(item)


def _dispatch_priority(item) -> int:
    """Preserve spoiler and text fast paths ahead of OCR."""
    if not isinstance(item, dict):
        return 2
    source = str(item.get("source_kind") or item.get("source") or "text").lower()
    if source in {"spoiler", "kjc_spoiler"}:
        return 0
    if source in {"text", "codevip"}:
        return 1
    return 2


def _drop_low_priority_oldest(self):
    """Prefer shedding stale OCR, never a newly arrived spoiler first."""
    if not self._queue:
        return None
    worst = max(_dispatch_priority(item) for item in self._queue)
    index = next(i for i, item in enumerate(self._queue)
                 if _dispatch_priority(item) == worst)
    item = self._queue[index]
    del self._queue[index]
    self.task_done()
    self.dropped_count += 1
    callback = self._on_drop
    if callback is not None:
        try:
            callback(item)
        except Exception:
            logger.exception("[Queue] drop callback failed")
    logger.warning("[Queue] pressure: dropped oldest lower-priority item: %s", _item_label(item))
    return item


class RandomCodeQueue(asyncio.Queue):
    """Draw the freshest message within the best source priority."""

    def __init__(self, maxsize=0):
        super().__init__(maxsize=maxsize)
        self._on_drop = None
        self.dropped_count = 0

    def set_drop_callback(self, callback):
        self._on_drop = callback

    drop_oldest = _drop_low_priority_oldest

    def _get(self):
        best = min(_dispatch_priority(item) for item in self._queue)
        eligible = [i for i, item in enumerate(self._queue)
                    if _dispatch_priority(item) == best]
        def age(index):
            item = self._queue[index]
            if not isinstance(item, dict):
                return (float('inf'), index)
            message_age = age_seconds(item.get('msg_ts') or item.get('message_date'))
            try:
                queued_age = max(0.0, time.perf_counter() - float(item['_queued_perf']))
            except (KeyError, TypeError, ValueError, OverflowError):
                queued_age = None
            ages = [value for value in (message_age, queued_age) if value is not None]
            return (max(ages) if ages else float('inf'), index)
        ages = {index: age(index)[0] for index in eligible}
        freshest = min(ages.values())
        # Pick at random within a short freshness window. This keeps new
        # spoiler/text ahead of stale work without always taking the first
        # code from a multi-code Telegram post.
        candidates = [index for index in eligible
                      if ages[index] <= freshest + 1.5]
        index = random.choice(candidates)
        item = self._queue[index]
        del self._queue[index]
        return item


class RandomBoundedCodeQueue(XX88SpoilerPriorityQueue):
    """Priority and freshness with original bounded overflow/recovery policy."""

    _put = asyncio.Queue._put
    _get = RandomCodeQueue._get
    drop_oldest = _drop_low_priority_oldest
