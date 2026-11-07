"""SC88 list parsing and distinct-account assignment; no network or submission."""
from __future__ import annotations

import random
import re
import string


DOMAIN = 'nhap-code-sc88.pages.dev'
CHANNEL_ID = -1004299442238
CHANNELS = {
    -1004299442238: {'name': 'SC88 | Kênh Chính Thức', 'username': 'kenhchinhthucSC88'},
    -1003067568809: {'name': 'SC88 | Phát Code Miễn Phí', 'username': 'sc88phatcode'},
}
ACCOUNTS = ('kaoboy012', 'conve99sau', 'minichan012')
_TOKEN = re.compile(r'^[A-Za-z0-9' + re.escape(string.punctuation) + r']+$')
_NON_ALNUM = re.compile(r'[^A-Za-z0-9]')


def _clean_list_token(token: str) -> str | None:
    if not 10 <= len(token) <= 16 or not _TOKEN.fullmatch(token):
        return None
    if '://' in token:
        return None
    cleaned = _NON_ALNUM.sub('', token)
    if len(cleaned) != 10:
        return None
    return cleaned


def extract_sc88_codes(text: str) -> list[str]:
    """Extract consecutive two-column code rows; never merge neighbouring tokens."""
    groups: list[list[str]] = []
    group: list[str] = []
    for line in str(text or '').splitlines():
        parts = line.strip().split()
        row = [_clean_list_token(part) for part in parts] if len(parts) == 2 else []
        if len(row) == 2 and all(row):
            group.extend(row)
        else:
            if len(group) >= 4:
                groups.append(group)
            group = []
    if len(group) >= 4:
        groups.append(group)
    seen: set[str] = set()
    result = []
    for group in groups:
        for code in group:
            if code not in seen:
                seen.add(code)
                result.append(code)
    return result


def assign_sc88_codes(codes, accounts=ACCOUNTS, *, sample=random.sample):
    """Choose distinct cleaned codes and assign each to at most one account."""
    unique = list(dict.fromkeys(str(code) for code in codes if code))
    names = list(dict.fromkeys(str(account).strip() for account in accounts if str(account).strip()))
    count = min(3, len(unique), len(names))
    return list(zip(names[:count], sample(unique, count)))
