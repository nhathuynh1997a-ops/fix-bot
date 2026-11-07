"""Configure AutoBot to attach to an EXISTING Microsoft Edge profile.

No Edge processes are started, stopped, reconfigured, or modified by this tool.
Only Edge-related lines in .env are updated (after a timestamped backup).
"""
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DOTENV = ROOT / '.env'


def _user_data_dir(raw: str) -> Path:
    raw = os.path.expandvars(raw.strip().strip('"').strip("'"))
    path = Path(raw).expanduser().resolve()
    # Edge's Settings -> Profile path / edge://version reports Default or
    # Profile N. DevToolsActivePort lives in the PARENT user-data directory.
    if (path / 'Local State').is_file():
        return path
    if (path.parent / 'Local State').is_file() and (
        path.name == 'Default' or re.fullmatch(r'Profile \d+', path.name, re.I)
    ):
        return path.parent
    raise ValueError(
        'Khong tim thay Edge User Data / Local State. Mo edge://version trong '
        'profile muon dung va lay duong dan Profile Path; co the truyen duong '
        'dan nay cho script. Khong tao thu muc profile moi.'
    )


def _configure_env(original: str, changes: dict[str, str]) -> str:
    lines = original.splitlines(keepends=True)
    pattern = re.compile(r'^\s*([A-Za-z_][A-Za-z_0-9]*)\s*=')
    seen = set()
    result = []
    for line in lines:
        m = pattern.match(line)
        if m and m.group(1) in changes:
            name = m.group(1)
            if name not in seen:
                result.append(f'{name}={changes[name]}\n')
                seen.add(name)
            continue
        result.append(line)
    if result and not result[-1].endswith(('\r', '\n')):
        result.append('\n')
    result.extend(f'{key}={value}\n' for key, value in changes.items() if key not in seen)
    return ''.join(result)


def configure(path: str) -> Path:
    if not DOTENV.is_file():
        raise FileNotFoundError(f'Khong tim thay {DOTENV}; vui long giu .env hien tai.')
    directory = _user_data_dir(path)
    # Do not modify credentials, session, site accounts, watchlist, or proxies.
    # EDGE_CDP_PORT is merely a fallback; the active port is always taken from
    # the DevToolsActivePort file for this user-data directory at runtime.
    changes = {
        'EDGE_ATTACH_USER_DATA_DIR': str(directory),
        'EDGE_AUTO_LAUNCH': 'false',
        'EDGE_KILL_ON_SHUTDOWN': 'false',
        'EDGE_CDP_HOST': '127.0.0.1',
        'EDGE_CDP_PORT': '9222',
        'EDGE2_ENABLED': 'false',
    }
    original = DOTENV.read_text(encoding='utf-8-sig')
    content = _configure_env(original, changes)
    backup = DOTENV.with_name(f'.env.backup_{datetime.now():%Y%m%d_%H%M%S}')
    if backup.exists():
        raise FileExistsError('Ten backup da ton tai, thu lai sau mot giay.')
    shutil.copy2(DOTENV, backup)
    tmp = DOTENV.with_name('.env.same_edge_profile.tmp')
    try:
        with tmp.open('w', encoding='utf-8', newline='') as output:
            output.write(content)
            output.flush()
            os.fsync(output.fileno())
        os.replace(tmp, DOTENV)
    finally:
        tmp.unlink(missing_ok=True)
    print('[OK] AutoBot se gan vao Edge profile dang mo. KHONG tao profile moi.')
    print(f'  User Data: {directory}')
    print(f'  Backup: {backup.name}')
    try:
        state = json.loads((directory / 'Local State').read_text(encoding='utf-8-sig'))
        last_used = state.get('profile', {}).get('last_used', '')
        if last_used:
            print(f'  Edge last-used profile: {last_used}')
    except (OSError, ValueError, TypeError):
        pass
    try:
        parts = (directory / 'DevToolsActivePort').read_text(encoding='utf-8').splitlines()
        if len(parts) < 2 or not parts[1].startswith('/devtools/browser/'):
            raise ValueError('Invalid Edge DevToolsActivePort')
        port = int(parts[0])
        if not 1 <= port <= 65535:
            raise ValueError('Invalid Edge DevTools port')
        print(f'  DevTools CDP active on localhost:{port}')
    except (OSError, ValueError, UnicodeError):
        print('  [CHUA BAT CDP] Trong CHINH profile Edge muon dung:')
        print('  1. Mo edge://inspect/#remote-debugging')
        print('  2. Bat Allow remote debugging for this browser instance')
        print('  3. Giu Edge mo, sau do chay diagnose_edge_cdp.bat va run.bat')
    print('  Khong dong Edge, khong xoa cookie, khong thay doi tai khoan.')
    return directory


def main() -> int:
    parser = argparse.ArgumentParser(description='Attach to an existing Edge profile, no new profile')
    parser.add_argument('--profile-path', default='', help='Folder shown by edge://version under Profile Path')
    args = parser.parse_args()
    raw = args.profile_path or os.path.join(os.getenv('LOCALAPPDATA', ''), 'Microsoft', 'Edge', 'User Data')
    if not raw or (not args.profile_path and not os.getenv('LOCALAPPDATA')):
        print('[LOI] LOCALAPPDATA chua co. Dung --profile-path "C:\\...\\User Data\\Default"')
        return 2
    try:
        configure(raw)
        return 0
    except (OSError, UnicodeError, ValueError) as exc:
        print(f'[LOI] {exc}')
        return 2


if __name__ == '__main__':
    sys.exit(main())
