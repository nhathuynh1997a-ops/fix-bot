"""Telegram MTProto Push admission: synchronous, bounded and network-free.

This module never reads history, polls Telegram or creates an asyncio task for
an incoming message. Telethon owns the persistent MTProto TCP connection and
calls the handlers; this receiver only checks metadata and enqueues to RAM.
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
import time
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Iterable
from urllib.parse import urlsplit
import config as _config_module

from config import Config
from ram_backpressure import PrioritizedIngressQueue, content_tier

_log = logging.getLogger("autobot." + __name__)
RAW_INGEST_QUEUE = PrioritizedIngressQueue(maxsize=Config.INGEST_QUEUE_MAXSIZE)


def _message_dedup_key(chat_id: int, message: Any, edited: bool) -> tuple:
    # A new edit must not be discarded as a duplicate of the original message.
    edit_dt = getattr(message, "edit_date", None)
    edit_ts = int(edit_dt.timestamp() * 1000000) if edit_dt else 0
    entities = tuple(
        (type(entity).__name__, getattr(entity, "offset", None),
         getattr(entity, "length", None), getattr(entity, "url", None))
        for entity in (getattr(message, "entities", None) or ())
    )
    text = str(getattr(message, "message", None) or getattr(message, "text", None) or "")
    digest = hashlib.blake2s(text.encode("utf-8", "surrogatepass"), digest_size=12).digest()
    media = getattr(message, "media", None)
    media_object = (getattr(media, "document", None) or getattr(media, "photo", None)
                    or getattr(media, "webpage", None))
    media_sig = (type(media).__name__, getattr(media_object, "id", None)) if media else None
    return (int(chat_id), int(message.id), bool(edited), edit_ts,
            entities, digest, media_sig)


def _ingress_age_limit(chat_id: int, cfg: Any, message: Any) -> float:
    channel = getattr(Config, "CHANNEL_CONFIG", {}).get(int(chat_id), {})
    if "xx88code.com" in str(channel.get("url", "")).lower():
        return max(0.0, float(getattr(cfg, "xx88_max_age_seconds", 40.0)))
    if content_tier(message) >= 2 and getattr(message, 'media', None) is not None and (
        int(chat_id) in getattr(Config, 'OCR_ALLOWED_CHANNEL_IDS', ())
        or channel.get('has_video', False)
    ):
        return max(0.0, float(getattr(cfg, 'ocr_max_age_seconds', 45.0)))
    return max(0.0, float(getattr(cfg, "max_age_seconds", 120.0)))


@dataclass(slots=True)
class Envelope:
    chat_id: int
    message: Any
    edited: bool
    source: str
    seen_perf: float
    age_s: float


class OfferResult(str, Enum):
    ACCEPTED = "accepted"
    DUPLICATE = "duplicate"
    FILTERED = "filtered"
    INVALID = "invalid"
    SINK_FAILED = "sink_failed"


@dataclass
class ReceiverConfig:
    dedup_size: int = 10000
    dedupe_ttl_seconds: float = 240.0
    age_drop_enabled: bool = True
    max_age_seconds: float = 120.0
    xx88_max_age_seconds: float = 40.0
    ocr_max_age_seconds: float = 45.0

    @classmethod
    def from_config(cls, cfg):
        return cls(
            dedup_size=min(10000, max(1, int(getattr(cfg, "TELEGRAM_INGRESS_DEDUPE_MAX_ENTRIES", 10000)))),
            dedupe_ttl_seconds=min(3600.0, max(1.0, float(getattr(cfg, "TELEGRAM_INGRESS_DEDUPE_TTL_SEC", 600.0)))),
            age_drop_enabled=bool(getattr(cfg, "TELEGRAM_INGRESS_AGE_DROP_ENABLED", True)),
            max_age_seconds=float(getattr(cfg, "INGRESS_MAX_AGE_SEC", 120.0)),
            xx88_max_age_seconds=float(getattr(cfg, "INGRESS_XX88_MAX_AGE_SEC", 40.0)),
            ocr_max_age_seconds=float(getattr(cfg, "INGRESS_OCR_MAX_AGE_SEC", 45.0)),
        )


class TelegramReceiver:
    """Fast event-loop-only admission; the sink MUST be a sync put_nowait.

    No await is allowed in the hot path: if downstream is full, shedding occurs
    immediately without blocking Telethon's updates dispatcher.
    """

    def __init__(self, client, chats: Iterable[int], sink: Callable, *, accept=None,
                 cfg=None, log=None, clock=time.monotonic):
        self._chat_set = set(map(int, chats))
        self._sink = sink
        self._accept = accept
        self._cfg = cfg or ReceiverConfig()
        self._log = log or _log
        self._clock = clock
        self._seen = OrderedDict()
        self._channel_cfg = {int(cid): dict(getattr(Config, 'CHANNEL_CONFIG', {}).get(int(cid), {}))
                             for cid in self._chat_set}
        self._domains = {cid: urlsplit(cfg.get('url', '')).hostname or ''
                         for cid, cfg in self._channel_cfg.items()}
        self._hours_check = getattr(_config_module, 'is_site_business_hours_open',
                                    lambda domain: True)
        self._st = dict(push=0, dup=0, rejected=0, age_drop=0, error=0)

    def stats(self):
        return dict(self._st)

    def _prune_seen(self):
        cutoff = self._clock() - self._cfg.dedupe_ttl_seconds
        while self._seen and (next(iter(self._seen.values())) <= cutoff
                              or len(self._seen) > self._cfg.dedup_size):
            self._seen.popitem(last=False)

    @staticmethod
    def _message_age(message, edited=False):
        date = (getattr(message, "edit_date", None) if edited else None) or getattr(message, "date", None)
        if date is None:
            return -1.0
        if date.tzinfo is None:
            date = date.replace(tzinfo=timezone.utc)
        return max(0.0, (datetime.now(timezone.utc) - date).total_seconds())

    def offer_nowait(self, chat_id, message, *, edited=False, source="mtproto_push"):
        """Return immediately; no RPC, no download, no long CPU or asyncio task."""
        try:
            cid = int(chat_id)
        except (TypeError, ValueError):
            return OfferResult.INVALID
        if cid not in self._chat_set or message is None or getattr(message, "id", None) is None:
            return OfferResult.INVALID
        current_channel = getattr(Config, 'CHANNEL_CONFIG', {}).get(cid)
        if current_channel is None or not current_channel.get('enabled', True):
            self._st['rejected'] += 1
            return OfferResult.FILTERED
        domain = self._domains.get(cid, '')
        if domain and not self._hours_check(domain):
            self._st['rejected'] += 1
            return OfferResult.FILTERED
        if self._accept is not None and not self._accept(message):
            self._st["rejected"] += 1
            return OfferResult.FILTERED
        key = _message_dedup_key(cid, message, edited)
        self._prune_seen()
        if key in self._seen:
            self._st["dup"] += 1
            return OfferResult.DUPLICATE
        age = self._message_age(message, edited)
        if self._cfg.age_drop_enabled and age > _ingress_age_limit(cid, self._cfg, message):
            self._st["age_drop"] += 1
            return OfferResult.FILTERED
        env = Envelope(cid, message, bool(edited), source, time.perf_counter(), age)
        try:
            self._sink(env)
        except asyncio.QueueFull:
            self._st["rejected"] += 1
            return OfferResult.SINK_FAILED
        except Exception:
            self._st["error"] += 1
            self._log.exception("[MTProtoPush] RAM admission error")
            return OfferResult.SINK_FAILED
        self._seen[key] = self._clock()
        self._prune_seen()
        self._st["push"] += 1
        return OfferResult.ACCEPTED

