"""Bounded, cancellation-safe media-DC sender creation for Telethon 1.x.

Uses the same export/import authorization flow as TelegramBaseClient, without
mutating the main connection's InitConnection request or leaking a sender when
creation is cancelled. No second home-DC client or disk session is created.
"""
import asyncio
import copy
import os
import random
import time
import json
import math
from pathlib import Path

from telethon import TelegramClient, functions
from telethon.network.mtprotosender import MTProtoSender
from telethon.tl.alltlobjects import LAYER
from telethon.client.downloads import DownloadMethods

from config import Config
from logger_setup import logger


def _env_flag(name, default):
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    return value.strip().lower() not in ('0', 'false', 'no', 'off')


def _env_seconds(name, default, minimum):
    try:
        return max(minimum, float(os.getenv(name, default)))
    except (TypeError, ValueError):
        return default


class ReliableTelegramClient(TelegramClient):
    # PATCH_MARKER_2026-11-02-media-dc-keep-warm
    # Giữ sẵn các kết nối tới máy chủ media (DC) giữa các lần tải. Trước đây mỗi
    # video phải mở lại 6 kết nối + đăng nhập từng cái (~1 giây/cái), mất 5-7 giây
    # với video nằm ở DC xa (vd LLwin ở DC1). Tắt nhanh: MEDIA_DC_KEEP_WARM=false
    # trong .env — khi đó mọi thứ chạy y như bản cũ (đóng kết nối sau mỗi lần tải).

    def _media_pool_enabled(self):
        return _env_flag('MEDIA_DC_KEEP_WARM', True)

    def _media_pool_state(self):
        if not hasattr(self, '_media_pool'):
            # (dc_id, lane) -> [sender, dùng_lần_cuối, còn_sống_lần_cuối]
            self._media_pool = {}
            self._media_pool_task = None
        return self._media_pool

    @staticmethod
    async def _media_close_sender(sender):
        if sender is None:
            return
        try:
            await asyncio.wait_for(sender.disconnect(), 5.0)
        except Exception:
            pass

    @staticmethod
    async def _media_sender_alive(sender, timeout):
        """Gửi ping thử; True nếu máy chủ trả lời kịp."""
        if sender is None or not sender.is_connected():
            return False
        try:
            future = sender.send(functions.PingRequest(ping_id=random.getrandbits(62)))
            done, _ = await asyncio.wait({future}, timeout=timeout)
            if not done or future.cancelled():
                if not future.done():
                    future.cancel()
                return False
            return future.exception() is None
        except asyncio.CancelledError:
            raise
        except Exception:
            return False

    async def _media_pool_take(self, dc_id, lane):
        """Lấy kết nối có sẵn (đã kiểm tra còn sống), hoặc None nếu phải tạo mới."""
        if not self._media_pool_enabled():
            return None
        entry = self._media_pool_state().pop((dc_id, lane), None)
        if entry is None:
            return None
        sender, used_at, alive_at = entry
        now = time.monotonic()
        ttl = _env_seconds('MEDIA_DC_KEEP_WARM_SECONDS', 14400.0, 60.0)
        if now - used_at > ttl or not sender.is_connected():
            await self._media_close_sender(sender)
            return None
        route = self._recent_media_route(dc_id)
        endpoint = getattr(sender, '_autobot_media_endpoint', None)
        performance = route.get('performance', {})
        preferred = tuple(route.get('preferred') or ())
        if endpoint and preferred and endpoint != preferred:
            current = performance.get(f'{endpoint[0]}:{endpoint[1]}', {}).get('seconds_per_mb', 0)
            best = performance.get(f'{preferred[0]}:{preferred[1]}', {}).get('seconds_per_mb', 0)
            if best > 0 and current > best * 2:
                await self._media_close_sender(sender)
                return None
        if now - alive_at > 8.0 and not await self._media_sender_alive(sender, 3.0):
            logger.debug('[Media-DC] kết nối cũ dc=%s lane=%s đã chết, mở cái mới', dc_id, lane)
            await self._media_close_sender(sender)
            return None
        logger.debug('[Media-DC] dùng lại kết nối có sẵn dc=%s lane=%s', dc_id, lane)
        return sender

    async def _media_pool_put(self, dc_id, lane, sender):
        """Trả kết nối về kho để lần tải sau dùng lại (hoặc đóng nếu không giữ được)."""
        if sender is None:
            return
        if not self._media_pool_enabled() or not sender.is_connected():
            await self._media_close_sender(sender)
            return
        pool = self._media_pool_state()
        now = time.monotonic()
        old = pool.get((dc_id, lane))
        pool[(dc_id, lane)] = [sender, now, now]
        if old is not None and old[0] is not sender:
            await self._media_close_sender(old[0])
        task = self._media_pool_task
        if task is None or task.done():
            self._media_pool_task = asyncio.get_running_loop().create_task(
                self._media_pool_keepalive())

    async def _media_pool_sweep_one(self, key):
        pool = self._media_pool_state()
        # Keep the warm connection available while a ping is in flight. A
        # downloader may take ownership during this await; the sweeper must
        # never close or put back a sender which has been borrowed meanwhile.
        entry = pool.get(key)
        if entry is None:
            return
        sender, used_at, _alive_at = entry
        ttl = _env_seconds('MEDIA_DC_KEEP_WARM_SECONDS', 14400.0, 60.0)
        healthy = (time.monotonic() - used_at <= ttl
                   and await self._media_sender_alive(sender, 5.0))
        if pool.get(key) is not entry:
            return
        if not healthy:
            pool.pop(key, None)
            logger.debug('[Media-DC] bỏ kết nối có sẵn dc=%s lane=%s (hết hạn hoặc đã chết)', *key)
            await self._media_close_sender(sender)
            return
        entry[2] = time.monotonic()

    async def _media_pool_keepalive(self):
        """Ping định kỳ để kết nối không bị đứt khi nghỉ; dọn cái hết hạn."""
        pool = self._media_pool_state()
        try:
            while pool:
                await asyncio.sleep(_env_seconds('MEDIA_DC_KEEPALIVE_SECONDS', 40.0, 5.0))
                await asyncio.gather(
                    *(self._media_pool_sweep_one(key) for key in list(pool)),
                    return_exceptions=True)
        finally:
            self._media_pool_task = None

    async def prewarm_media_dc(self, dc_id, lanes):
        """Mở sẵn kết nối tới media DC ngay khi bot khởi động, để video đầu tiên không phải chờ."""
        if not self._media_pool_enabled():
            return 0
        pool = self._media_pool_state()
        async def warm_lane(lane):
            if (dc_id, lane) in pool:
                return True
            try:
                sender = await self._create_exported_sender(dc_id, lane=lane)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning('[Media-DC] prewarm dc=%s lane=%s failed: %s', dc_id, lane, type(exc).__name__)
                return False
            sender.dc_id = dc_id
            await self._media_pool_put(dc_id, lane, sender)
            return True
        # TCP handshakes are independent. Export/import authorization remains
        # paired under _media_authorize_lock inside the sender factory.
        ready = sum(await asyncio.gather(*(warm_lane(lane) for lane in range(max(0, int(lanes))))))
        logger.info('[Media-DC] prewarm dc=%s ready=%s/%s', dc_id, ready, lanes)
        return ready

    async def _close_media_pool(self):
        task = getattr(self, '_media_pool_task', None)
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        if getattr(self, '_media_route_state', None):
            self._save_media_routes()
        pool = getattr(self, '_media_pool', None)
        if not pool:
            return
        entries = list(pool.values())
        pool.clear()
        await asyncio.gather(*(self._media_close_sender(e[0]) for e in entries),
                             return_exceptions=True)

    async def _disconnect_coro(self):
        prewarm = getattr(self, '_media_prewarm_task', None)
        if prewarm is not None and not prewarm.done():
            prewarm.cancel()
            await asyncio.gather(prewarm, return_exceptions=True)
        try:
            await self._close_media_pool()
        except Exception:
            pass
        await super()._disconnect_coro()

    def _media_routes(self):
        if not hasattr(self, '_media_route_state'):
            self._media_route_state = {}
            path = str(getattr(Config, 'MEDIA_DC_ROUTE_STATE_PATH', '') or '')
            try:
                saved = json.loads(Path(path).read_text(encoding='utf-8')) if path else {}
                if isinstance(saved, dict):
                    self._media_route_state = {key: value for key, value in saved.items()
                                               if isinstance(value, dict)}
            except (OSError, ValueError, TypeError):
                pass
        return self._media_route_state

    def _save_media_routes(self):
        path = str(getattr(Config, 'MEDIA_DC_ROUTE_STATE_PATH', '') or '')
        if not path:
            return
        try:
            target = Path(path)
            target.parent.mkdir(parents=True, exist_ok=True)
            temp = target.with_suffix(target.suffix + '.tmp')
            temp.write_text(json.dumps(self._media_routes(), indent=2), encoding='utf-8')
            temp.replace(target)
        except OSError:
            logger.warning('[Media-DC] route hint could not be saved')

    def _recent_media_route(self, dc_id):
        value = self._media_routes().get(str(dc_id), {})
        try:
            if isinstance(value, dict) and 0 <= time.time() - value.get('updated_at', 0) < 86400:
                return value
        except TypeError:
            pass
        return {}

    async def _get_dc(self, dc_id, cdn=False):
        primary = await super()._get_dc(dc_id, cdn=cdn)
        if cdn:
            return primary
        route = self._recent_media_route(dc_id)
        options = [d for d in getattr(self.__class__._config, 'dc_options', ())
                   if d.id == dc_id and not d.cdn and bool(d.ipv6) == self._use_ipv6
                   and not bool(getattr(d, 'media_only', False))]
        # Home/update RPCs must not inherit a media-only option merely because
        # it is listed first by Telegram. The media factory selects its own.
        if bool(getattr(primary, 'media_only', False)) and options:
            primary = options[0]
        # Hints are only accepted if Telegram's current config still advertises
        # that address; a local file cannot redirect authorization elsewhere.
        preferred, failed = route.get('preferred'), route.get('failed')
        for dc in options:
            endpoint = [dc.ip_address, dc.port]
            if endpoint == preferred and endpoint != failed:
                return dc
        if [primary.ip_address, primary.port] == failed:
            return next((d for d in options if [d.ip_address, d.port] != failed), primary)
        return primary

    def new_media_download_channel(self, dc_id, lane=0, chat_id=None, message_id=None):
        return MediaDownloadChannel(self, dc_id, lane, chat_id, message_id)

    def note_media_sender_failed(self, dc_id, sender, *, since=None):
        endpoint = getattr(sender, '_autobot_media_endpoint', None)
        successes = getattr(self, '_media_last_success', {})
        if since is not None and successes.get((dc_id, endpoint), float('-inf')) > since:
            logger.info('[Media-DC] isolated channel stall dc=%s; endpoint still delivers verified bytes', dc_id)
            return
        failed = getattr(self, '_media_failed_endpoints', None)
        if failed is None:
            failed = self._media_failed_endpoints = {}
        failed[dc_id] = getattr(sender, '_autobot_media_endpoint', None)
        if failed[dc_id]:
            state = self._media_routes().setdefault(str(dc_id), {})
            state.update(failed=list(failed[dc_id]), updated_at=time.time())
            self._save_media_routes()

    def note_media_download_success(self, dc_id, sender, *, elapsed_seconds=None, size_bytes=None):
        endpoint = getattr(sender, '_autobot_media_endpoint', None)
        if not endpoint:
            return
        if not hasattr(self, '_media_last_success'):
            self._media_last_success = {}
        self._media_last_success[(dc_id, endpoint)] = time.perf_counter()
        failed = getattr(self, '_media_failed_endpoints', {})
        if failed.get(dc_id) == endpoint:
            failed.pop(dc_id, None)
        state = self._media_routes().setdefault(str(dc_id), {})
        previous_preferred = state.get('preferred')
        measured = bool(elapsed_seconds and size_bytes and size_bytes >= 128 * 1024)
        if measured:
            speed_cost = elapsed_seconds * 1_000_000 / size_bytes
            if math.isfinite(speed_cost) and speed_cost > 0:
                performance = state.setdefault('performance', {})
                key = f'{endpoint[0]}:{endpoint[1]}'
                old = performance.get(key, {})
                cost = old.get('seconds_per_mb', speed_cost) * .7 + speed_cost * .3
                performance[key] = {'seconds_per_mb': cost, 'samples': old.get('samples', 0) + 1}
                # Last to finish is often the slowest. Never promote an address
                # just because its delayed chunk happened to complete last.
                valid = {(dc.ip_address, dc.port) for dc in getattr(self.__class__._config, 'dc_options', ())
                         if dc.id == dc_id and not dc.cdn and bool(dc.ipv6) == self._use_ipv6}
                scored = [(row['seconds_per_mb'], address) for address in valid
                          if (row := performance.get(f'{address[0]}:{address[1]}'))
                          and isinstance(row.get('seconds_per_mb'), (int, float))
                          and row['seconds_per_mb'] > 0]
                if scored:
                    state['preferred'] = list(min(scored)[1])
        if measured and state.get('preferred'):
            endpoint_to_save = state['preferred']
        else:
            endpoint_to_save = list(endpoint)
        if (state.get('failed') != list(endpoint) and previous_preferred == endpoint_to_save
                and time.time() - state.get('updated_at', 0) < 60):
            return
        state.update(preferred=endpoint_to_save, updated_at=time.time())
        if state.get('failed') == list(endpoint):
            state.pop('failed', None)
        self._save_media_routes()

    async def _create_exported_sender(self, dc_id, *, lane=0):
        primary = await self._get_dc(dc_id)
        candidates = [primary]
        config = getattr(self.__class__, '_config', None)
        for dc in getattr(config, 'dc_options', ()):
            if (dc.id == dc_id and not dc.cdn and bool(dc.ipv6) == self._use_ipv6
                    and all((dc.ip_address, dc.port) != (d.ip_address, d.port) for d in candidates)):
                candidates.append(dc)
        # Telegram advertises distinct file-transfer access points. Persisted
        # success on a generic RPC address must not pin downloads to that
        # address when media-only options exist (core.telegram.org/api/files).
        media_candidates = [dc for dc in candidates if bool(getattr(dc, 'media_only', False))]
        if media_candidates:
            candidates = media_candidates
        route = self._recent_media_route(dc_id)
        failed = getattr(self, '_media_failed_endpoints', {}).get(dc_id)
        if not failed and route.get('failed'):
            failed = tuple(route['failed'])
        preferred = tuple(route.get('preferred') or ())
        performance = route.get('performance', {})
        measured_preference = bool(preferred and isinstance(performance, dict)
                                   and performance.get(f'{preferred[0]}:{preferred[1]}'))
        if failed:
            candidates.sort(key=lambda dc: (dc.ip_address, dc.port) == failed)
        elif measured_preference and any((dc.ip_address, dc.port) == preferred for dc in candidates):
            candidates.sort(key=lambda dc: (dc.ip_address, dc.port) != preferred)
        elif len(candidates) > 1:
            offset = lane % len(candidates)
            candidates = candidates[offset:] + candidates[:offset]
        timeout = max(5.0, float(getattr(Config, 'MEDIA_DC_SETUP_TIMEOUT_SECONDS', 20.0)))
        for index, dc in enumerate(candidates[:2]):
            sender = MTProtoSender(None, loggers=self._log, retries=1,
                                  auto_reconnect=False, connect_timeout=timeout)
            sender._autobot_media_endpoint = (dc.ip_address, dc.port)
            phase = 'connect'
            started = time.perf_counter()

            async def connect_and_authorize():
                nonlocal phase
                await sender.connect(self._connection(
                    dc.ip_address, dc.port, dc.id, loggers=self._log,
                    proxy=self._proxy, local_addr=self._local_addr))
                if not hasattr(self, '_media_authorize_lock'):
                    self._media_authorize_lock = asyncio.Lock()
                phase = 'auth_queue'
                # A later export can supersede an earlier authorization blob.
                # Keep each export/import pair together; data transfers remain
                # independent after their transports have been authorized.
                async with self._media_authorize_lock:
                    phase = 'export_auth'
                    auth = await self(functions.auth.ExportAuthorizationRequest(dc_id))
                    phase = 'import_auth'
                    init = copy.copy(self._init_request)
                    # ImportAuthorization is not a file query. Explicitly keep
                    # this transfer-only connection from subscribing to updates;
                    # its MTProtoSender deliberately has no updates_queue.
                    init.query = functions.auth.ImportAuthorizationRequest(id=auth.id, bytes=auth.bytes)
                    # Match Telethon's receive_updates=False handshake: the
                    # entire InitConnection must run without subscribing,
                    # rather than wrapping only the nested auth RPC.
                    await sender.send(functions.InvokeWithLayerRequest(
                        LAYER, functions.InvokeWithoutUpdatesRequest(init)))

            try:
                await asyncio.wait_for(connect_and_authorize(), timeout)
                logger.info('[Media-DC] ready dc=%s endpoint=%s:%s attempt=%s setup_ms=%.1f media_only=%s lane=%s updates=disabled',
                            dc_id, dc.ip_address, dc.port, index + 1,
                            (time.perf_counter() - started) * 1000, bool(getattr(dc, 'media_only', False)), lane)
                return sender
            except BaseException as exc:
                try:
                    await asyncio.wait_for(sender.disconnect(), 5.0)
                except Exception:
                    logger.warning('[Media-DC] sender cleanup failed dc=%s', dc_id)
                transient = isinstance(exc, (TimeoutError, ConnectionError, OSError))
                if not transient or index + 1 >= min(2, len(candidates)):
                    raise
                logger.warning('[Media-DC] retry fresh endpoint dc=%s phase=%s error=%s',
                               dc_id, phase, type(exc).__name__)


class MediaDownloadChannel:
    """One media-only sender per lane, using Telethon's iterator/error handling."""
    def __init__(self, client, dc_id, lane, chat_id, message_id):
        self.client = client
        self.dc_id = dc_id
        self.lane = lane
        self.msg_data = (chat_id, message_id) if chat_id and message_id else None
        self.senders = {}
        self._active_dc_id = dc_id

    def __getattr__(self, name):
        return getattr(self.client, name)

    async def prepare(self):
        await self._borrow_exported_sender(self.dc_id)

    async def _borrow_exported_sender(self, dc_id):
        if dc_id not in self.senders:
            sender = await self.client._media_pool_take(dc_id, self.lane)
            if sender is None:
                sender = await self.client._create_exported_sender(dc_id, lane=self.lane)
            sender.dc_id = dc_id
            self.senders[dc_id] = sender
        self._active_dc_id = dc_id
        return self.senders[dc_id]

    async def _return_exported_sender(self, sender):
        # The lane owns the transport until every one of its iterators closes.
        pass

    def iter_download(self, file, **kwargs):
        return DownloadMethods._iter_download(self, file, msg_data=self.msg_data, **kwargs)

    def note_media_download_success(self, dc_id, **timings):
        self.client.note_media_download_success(self._active_dc_id, self.failed_sender(dc_id), **timings)

    def failed_sender(self, dc_id):
        return self.senders.get(self._active_dc_id)

    async def recover(self, *, since):
        sender = self.failed_sender(self.dc_id)
        if sender is not None:
            self.client.note_media_sender_failed(self._active_dc_id, sender, since=since)
        # Only this lane owns these senders; healthy lanes keep downloading.
        await self.aclose()

    async def release(self):
        """Tải xong an toàn: trả kết nối về kho để video sau dùng lại."""
        senders, self.senders = self.senders, {}
        await asyncio.gather(*(self.client._media_pool_put(dc_id, self.lane, sender)
                               for dc_id, sender in senders.items()),
                             return_exceptions=True)

    async def aclose(self):
        await asyncio.gather(*(asyncio.wait_for(s.disconnect(), 5.0)
                               for s in self.senders.values()), return_exceptions=True)
        self.senders.clear()
