"""OCR chạy song song khi video đang tải (progressive OCR).

Ý tưởng: tải multipart theo thứ tự chunk nên phần ĐẦU file luôn liền mạch trước. Khi phần đầu
đã chứa trọn hộp `moov` (video "faststart") và đủ dài để chứa mốc quét, ta cắt phần đầu đó ra
một file thử (probe) và chạy OCR ngay, trong lúc các chunk còn lại vẫn đang tải. Mã tìm thấy
được phát sớm qua on_code (cùng đường EarlyDispatch hiện có); khi tải xong, lượt OCR đầy đủ
vẫn chạy như cũ và chỉ bỏ qua mã đã phát. Nếu file không faststart thì không probe -> hành vi cũ.
"""
from __future__ import annotations

import asyncio
import os
import subprocess
import threading
import time
from typing import Callable, Optional

from logger_setup import logger


def mp4_moov_in_prefix(path: str, prefix_bytes: int) -> bool:
    """True nếu hộp `moov` nằm trọn trong prefix_bytes đầu và đứng TRƯỚC `mdat`."""
    try:
        with open(path, "rb") as handle:
            pos = 0
            while pos + 8 <= prefix_bytes:
                handle.seek(pos)
                header = handle.read(16)
                if len(header) < 8:
                    return False
                size = int.from_bytes(header[:4], "big")
                kind = header[4:8]
                if size == 1:
                    if len(header) < 16:
                        return False
                    size = int.from_bytes(header[8:16], "big")
                elif size == 0:
                    return False  # hộp kéo tới hết file: không phải moov-trước
                if size < 8:
                    return False
                if kind == b"moov":
                    return pos + size <= prefix_bytes
                if kind == b"mdat":
                    return False  # dữ liệu trước moov -> không thể giải mã phần đầu
                pos += size
    except OSError:
        return False
    return False


def mp4_tail_moov(path: str, prefix_bytes: int, tail_start: int, total: int):
    """Find a complete tail moov using only known completed byte ranges."""
    try:
        with open(path, 'rb') as handle:
            pos, mdat = 0, None
            for _ in range(128):
                if not (pos + 16 <= prefix_bytes or tail_start <= pos and pos + 16 <= total):
                    return None
                handle.seek(pos)
                header = handle.read(16)
                size, kind = int.from_bytes(header[:4], 'big'), header[4:8]
                extended = size == 1
                if extended:
                    size = int.from_bytes(header[8:16], 'big')
                if size < (16 if extended else 8) or pos + size > total:
                    return None
                if kind == b'mdat':
                    if mdat is not None or pos + (16 if extended else 8) >= prefix_bytes:
                        return None
                    mdat = (pos, extended)
                elif kind == b'moov':
                    if mdat is not None and pos >= tail_start and pos + size <= total:
                        return mdat[0], mdat[1], pos, size
                    return None
                pos += size
    except (OSError, ValueError):
        pass
    return None


def safe_partial_video_seconds(path: str) -> float:
    """Bound reads to complete video packets; return 0 when metadata is uncertain."""
    try:
        total = os.path.getsize(path)
        with open(path, 'rb') as handle:
            pos = 0
            data_end = None
            for _ in range(128):
                if pos + 16 > total:
                    break
                handle.seek(pos)
                header = handle.read(16)
                size, kind = int.from_bytes(header[:4], 'big'), header[4:8]
                if size == 1:
                    size = int.from_bytes(header[8:16], 'big')
                if size < 8:
                    break
                if kind == b'mdat':
                    data_end = min(total, pos + size)
                    break
                pos += size
        if data_end is None:
            return 0.0
        proc = subprocess.run(
            ['ffprobe', '-v', 'quiet', '-select_streams', 'v:0',
             '-show_entries', 'packet=pts_time,pos,size', '-of', 'csv=p=0', path],
            capture_output=True, text=True, timeout=4, check=False,
            creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0),
        )
        latest = 0.0
        last_pos = -1
        for line in proc.stdout.splitlines():
            timestamp, size, offset = line.split(',')
            timestamp, size, offset = float(timestamp), int(size), int(offset)
            if size <= 0 or offset < last_pos or offset + size > data_end:
                break
            latest = max(latest, timestamp)
            last_pos = offset
        # Leave room for video frame reordering and an independent second frame.
        return max(0.0, latest - 1.0)
    except (OSError, ValueError, TypeError, subprocess.SubprocessError):
        return 0.0


class ProgressiveOCR:
    """Nhận tiến độ tải, quyết định lúc nào quét phần đầu file và phát mã sớm.

    run_probe(probe_path, partial_cfg) là coroutine do người gọi cung cấp: nó chạy OCR (đã giữ
    slot OCR) trên file thử và gọi `emit` cho mỗi mã. ProgressiveOCR chỉ điều phối + khử trùng.
    """

    def __init__(
        self,
        *,
        tmp_dir: str,
        extension: str,
        on_code: Callable[[dict], None],
        run_probe: Callable[[str, Callable[[dict], None]], "asyncio.Future"],
        max_probes: int = 3,
        first_fraction: float = 0.58,
        single_code: bool = False,
    ):
        self._tmp_dir = tmp_dir
        self._ext = extension or ".mp4"
        self._on_code = on_code
        self._run_probe = run_probe
        self._max_probes = max(1, int(max_probes))
        self._first_fraction = float(first_fraction)
        self._single = bool(single_code)
        self._loop = asyncio.get_running_loop()
        self._task: Optional[asyncio.Task] = None
        self._probes = 0
        self._last_probe_bytes = 0
        self._moov_ok: Optional[bool] = None
        self._tail_start: Optional[int] = None
        self.emitted: set[str] = set()
        self._emit_lock = threading.Lock()
        self._accept_emissions = True
        self.closed = False

    # --- phát mã (gọi từ luồng OCR) ---------------------------------------------------
    def emit(self, item: dict) -> None:
        code = str((item or {}).get("code") or "").strip()
        with self._emit_lock:
            if not self._accept_emissions or not code or code in self.emitted:
                return
            if self._single and self.emitted:
                return
            self._on_code(dict(item))
            self.emitted.add(code)

    # --- nhận tiến độ tải (gọi trên event loop) ---------------------------------------
    def on_progress(self, partial_path: str, prefix_bytes: int, total: int) -> None:
        if self.closed or total <= 0 or self._probes >= self._max_probes:
            return
        if self._task is not None and not self._task.done():
            return
        if self._single and self.emitted:
            return
        fraction = prefix_bytes / total
        if fraction >= 0.995:
            return  # sắp xong: để lượt OCR đầy đủ làm
        first = self._probes == 0
        need = self._first_fraction if first else self._last_probe_bytes / total + 0.18
        if fraction < need:
            return
        if self._moov_ok is None or not self._moov_ok:
            self._moov_ok = mp4_moov_in_prefix(partial_path, prefix_bytes)
            tail_moov = (mp4_tail_moov(partial_path, prefix_bytes, self._tail_start, total)
                         if self._tail_start is not None else None)
            if not self._moov_ok and tail_moov is None:
                return
        else:
            tail_moov = None
        self._probes += 1
        self._last_probe_bytes = prefix_bytes
        self._task = self._loop.create_task(self._probe(partial_path, prefix_bytes, total, tail_moov))

    def on_tail_ready(self, partial_path: str, prefix_bytes: int, tail_start: int, total: int) -> None:
        self._tail_start = tail_start
        self.on_progress(partial_path, prefix_bytes, total)

    async def _probe(self, partial_path: str, prefix_bytes: int, total: int, tail_moov=None) -> None:
        probe_path = os.path.join(self._tmp_dir, f"probe_{self._probes}{self._ext}")
        started = time.perf_counter()
        try:
            if tail_moov is None:
                await self._loop.run_in_executor(None, self._copy_prefix, partial_path, probe_path, prefix_bytes)
            else:
                await self._loop.run_in_executor(None, self._copy_prefix_with_tail_moov,
                                                 partial_path, probe_path, prefix_bytes, tail_moov)
            logger.info("⚡ [OCR-PROGRESSIVE] probe #%s %.0f%% (%s/%s bytes) khi đang tải",
                        self._probes, 100.0 * prefix_bytes / total, prefix_bytes, total)
            await self._run_probe(probe_path, self.emit)
            logger.info("⚡ [OCR-PROGRESSIVE] probe #%s xong %.0fms, đã phát sớm=%s",
                        self._probes, (time.perf_counter() - started) * 1000, sorted(self.emitted))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.info("[OCR-PROGRESSIVE] probe #%s bỏ qua: %s: %s", self._probes, type(exc).__name__, exc)
        finally:
            try:
                os.unlink(probe_path)
            except OSError:
                pass

    @staticmethod
    def _copy_prefix(src: str, dst: str, nbytes: int) -> None:
        remaining = nbytes
        with open(src, "rb") as fin, open(dst, "wb") as fout:
            while remaining > 0:
                block = fin.read(min(1024 * 1024, remaining))
                if not block:
                    break
                fout.write(block)
                remaining -= len(block)

    @classmethod
    def _copy_prefix_with_tail_moov(cls, src: str, dst: str, nbytes: int, info) -> None:
        mdat_pos, extended, moov_pos, moov_size = info
        cls._copy_prefix(src, dst, nbytes)
        with open(src, 'rb') as fin, open(dst, 'r+b') as out:
            # Preserve original sample offsets; truncate mdat at downloaded data.
            out.seek(mdat_pos + (8 if extended else 0))
            out.write((nbytes - mdat_pos).to_bytes(8 if extended else 4, 'big'))
            fin.seek(moov_pos)
            moov = fin.read(moov_size)
            if len(moov) != moov_size:
                raise IOError('incomplete tail moov')
            out.seek(nbytes)
            out.write(moov)

    async def finish(self, timeout: float = 5.0) -> None:
        """Gọi khi tải xong/lỗi: đợi probe đang chạy (nếu có) rồi chặn probe mới.
        PATCH_MARKER_2026-11-03-checkbot-fix: 25s cu qua dai — sau khi tai
        that bai, worker bi giu them toi 25s truoc khi xu ly ma tiep theo,
        trong khi probe OCR binh thuong chi ~1-2s. 5s du cho probe sap xong,
        va gioi han thiet hai khi probe ket (cho CPU, file do)."""
        self.closed = True
        task = self._task
        if task is not None and not task.done():
            try:
                await asyncio.wait_for(asyncio.shield(task), timeout=timeout)
            except asyncio.TimeoutError:
                with self._emit_lock:
                    self._accept_emissions = False
                task.cancel()
            except Exception:
                pass
