"""Read-only diagnostic of the configured Edge CDP listener and WebSocket."""
from __future__ import annotations

import asyncio
import sys

import chrome_launcher
from config import Config


async def main():
    from playwright.async_api import async_playwright

    host = str(Config.EDGE_CDP_HOST)
    attach = str(Config.EDGE_ATTACH_USER_DATA_DIR or '').strip()
    port = int(Config.EDGE_CDP_PORT)
    if attach:
        try:
            port, _ = chrome_launcher.devtools_active_port(attach)
        except (OSError, UnicodeError, ValueError) as exc:
            print('[FAIL] No valid DevToolsActivePort for the selected personal Edge profile.')
            print(f'  Detail: {type(exc).__name__}')
            print('  In Edge open edge://inspect/#remote-debugging and allow debugging,')
            print('  Run use_existing_edge_profile.bat once if not configured yet.')
            return 2
    print(f'Mode: {"Attach EXISTING Edge profile (no new window)" if attach else "Dedicated Edge / port"}')
    if attach:
        print(f'Existing Edge User Data: {attach}')
    print(f'CDP port: {host}:{port}')
    print(f'Port owner: {chrome_launcher.listener_identity(port)}')
    try:
        await asyncio.to_thread(chrome_launcher.assert_edge_endpoint, port, host, 2.0)
    except Exception as exc:
        print(f'[FAIL] CDP endpoint: {exc}')
        return 2
    print('[OK] Edge CDP endpoint responds to initial probe.')
    print('Dang ket noi Playwright (cho toi da 60 giay). Neu Edge hien hop thoai')
    print('"Allow remote debugging" / "Cho phep" thi bam Allow / Cho phep.')
    try:
        url = chrome_launcher.cdp_endpoint_url(port, host)
        async with async_playwright() as playwright:
            # Khong gui header Origin va KHONG retry voi Origin rong: Chromium tu choi
            # moi Origin co mat (ke ca rong) nen lan retry chi che mat loi that cua lan dau.
            browser = await playwright.chromium.connect_over_cdp(url, timeout=60000)
            try:
                session = await browser.new_browser_cdp_session()
                version = await session.send('Browser.getVersion')
                await session.detach()
                browser_string = str(version.get('product', '')) + ' ' + str(version.get('userAgent', ''))
                if 'Edg/' not in browser_string and 'Edge/' not in browser_string:
                    print('[FAIL] Connected browser is not Microsoft Edge.')
                    return 2
                print(f'[OK] Playwright connected to Microsoft Edge ({len(browser.contexts)} contexts).')
            finally:
                await browser.close()  # disconnect; never closes user's Edge process
    except Exception as exc:
        text = str(exc)
        low = text.lower()
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        shown = ' | '.join(ln for ln in lines if not ln.startswith(('Call log', '- <ws')))[:300]
        print(f'[FAIL] Playwright WebSocket attach: {type(exc).__name__}: {shown}')
        if '403' in text and 'websocket' in low:
            if 'rejected an incoming websocket connection' in low or '--remote-allow-origins' in low:
                print('  -> Edge CHAN ket noi vi header Origin. Khoi dong Edge voi --remote-allow-origins')
                print('     hoac dung profile bot rieng (enable_dedicated_edge.bat).')
            else:
                print('  -> Edge TU CHOI ket noi (Connection rejected). Day KHONG phai loi Origin.')
                print('     1) Mo 1 cua so Edge cua DUNG profile nay va giu no mo.')
                print('     2) Chay lai file nay; khi hop thoai cho phep remote debugging hien ra, bam Allow / Cho phep.')
                print('     3) Neu truoc do da bam khong cho phep: vao edge://inspect/#remote-debugging va bat lai.')
        elif 'timeout' in low:
            print('  -> Het thoi gian cho. Edge co the dang hien hop thoai cho phep: bam Allow / Cho phep roi chay lai.')
        return 2
    return 0


if __name__ == '__main__':
    sys.exit(asyncio.run(main()))