"""One-time, opt-in migration to a dedicated Microsoft Edge debugging profile.

Never edits an existing Edge profile or copies browser cookies. Preserves .env.
"""
from __future__ import annotations

import os
import re
import shutil
import socket
import sys
from datetime import datetime
from pathlib import Path

BASE = Path(__file__).resolve().parent
DOTENV = BASE / '.env'


def _is_free(port: int) -> bool:
    try:
        with socket.create_connection(('127.0.0.1', port), timeout=0.25):
            return False
    except OSError:
        return True


def main() -> int:
    if not DOTENV.is_file():
        print('[ERROR] No .env found. Keep your current settings or create .env first.')
        return 2
    available = [port for port in (1941, 1942, 1943, 1931) if _is_free(port)]
    if not available:
        print('[ERROR] Ports 1941-1943 and 1931 are all busy. Do not terminate personal Edge automatically.')
        return 2
    port = available[0]
    changes = {
        'EDGE_CDP_HOST': '127.0.0.1',
        'EDGE_CDP_PORT': str(port),
        'EDGE_ATTACH_USER_DATA_DIR': '',
        'EDGE_AUTO_LAUNCH': 'true',
        'EDGE_KILL_ON_SHUTDOWN': 'false',
        'EDGE_PROFILE_1': f'edge_bot_profile_{port}',
    }
    try:
        original = DOTENV.read_text(encoding='utf-8-sig')
    except UnicodeError:
        print('[ERROR] .env must be UTF-8. The file was not changed.')
        return 2
    # The new settings are chosen only after user explicitly launches this tool.
    lines = original.splitlines(keepends=True)
    seen: set[str] = set()
    output = []
    expression = re.compile(r'^(\s*)([A-Za-z_][A-Za-z0-9_]*)\s*=')
    for line in lines:
        match = expression.match(line)
        if match and match.group(2) in changes:
            name = match.group(2)
            if name not in seen:
                output.append(f'{name}={changes[name]}\n')
                seen.add(name)
            # Remove duplicate definitions for the modified keys.
            continue
        output.append(line)
    if output and not output[-1].endswith(('\n', '\r')):
        output.append('\n')
    for name, value in changes.items():
        if name not in seen:
            output.append(f'{name}={value}\n')
    backup = DOTENV.with_name(f'.env.backup_{datetime.now():%Y%m%d_%H%M%S}')
    if backup.exists():
        print('[ERROR] Backup name already exists; retry the command.')
        return 2
    shutil.copy2(DOTENV, backup)
    candidate = DOTENV.with_name('.env.autobot_tmp')
    try:
        with candidate.open('w', encoding='utf-8', newline='') as file:
            file.write(''.join(output))
            file.flush()
            os.fsync(file.fileno())
        os.replace(candidate, DOTENV)
    finally:
        if candidate.exists():
            candidate.unlink()
    print('[OK] Dedicated Edge profile enabled.')
    print(f'  CDP: http://127.0.0.1:{port}')
    print(f'  Profile: {BASE / f"edge_bot_profile_{port}"}')
    print(f'  Previous settings saved locally: {backup.name}')
    print('  Existing Microsoft Edge windows, cookies and profiles were NOT modified.')
    print('  Log in to each website once in the NEW Edge bot profile.')
    print('  Then run run.bat. Do not start a second bot with the same session.')
    return 0


if __name__ == '__main__':
    sys.exit(main())
