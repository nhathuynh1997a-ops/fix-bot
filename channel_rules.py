from __future__ import annotations

from config import Config


def _is_ocr_only_channel(chat_id) -> bool:
    try:
        return int(chat_id) in set(getattr(Config, "OCR_MEDIA_ONLY_CHANNEL_IDS", ()))
    except (TypeError, ValueError):
        return False


def _is_ocr_allowed_channel(chat_id, event=None) -> bool:
    try:
        channel = int(chat_id)
    except (TypeError, ValueError):
        return False
    for name in ("OCR_ALLOWED_CHANNEL_IDS", "OCR_MEDIA_ONLY_CHANNEL_IDS", "OCR_MEDIA_FALLBACK_CHANNEL_IDS"):
        if channel in (getattr(Config, name, ()) or ()):
            return True
    message = getattr(event, "message", None)
    media = getattr(event, "media", None) or getattr(message, "media", None)
    return bool(getattr(Config, "OCR_FALLBACK_ALL_MEDIA", False) and media)
