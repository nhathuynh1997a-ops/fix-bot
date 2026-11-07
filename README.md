# AutoBot - MTProto 4 tầng (Push + Raw + Active Difference)

**Luồng nhận mới:** socket TCP MTProto thường trực do Telethon quản lý, kết hợp đối chiếu `GetChannelDifference` cho kênh ưu tiên cao. Đây **không phải** WebSocket hay HTTP polling; các RPC Difference chạy tách khỏi handler.

```text
Telegram DC
    │  kết nối TCP MTProto (Telethon / ConnectionTcpAbridged)
    ▼
NewMessage / MessageEdited / Raw [đăng ký trước khi client.start()]
    │ kiểm tra channel ID, tuổi tin, chống lặp (RAM) - KHÔNG await
    ▼
Bounded Priority RAM Queue (INGEST_QUEUE_MAXSIZE, mặc định 128)
    │ ưu tiên: spoiler → text → image → video; sau đó priority của kênh
    │ đầy: thay tin cũ cùng/thấp ưu tiên, hoặc từ chối tin mới thấp ưu tiên
    ▼
Message Workers (MESSAGE_WORKERS)
    ├── text/spoiler -> trích xuất mã
    └── media -> download timeout -> OCR ảnh/video có giới hạn worker/queue
                                  │
                                  ▼
                      RAM queue theo website
                                  │
                                  ▼
                         Site workers + khóa tài khoản/tab
                                  │
                                  ▼
                         Playwright/HTTP -> submit -> kết quả
```

## Tích hợp

- `mtproto_push.py`: bộ nối MTProto Push/Raw sang RAM. Callback không tải media, OCR hay gửi mã.
- `mtproto_channel_diff.py`: prewarm peer/PTS và vòng RPC Difference giới hạn theo kênh ưu tiên.
- `tg_ingest.py`: tiếp nhận đồng bộ với `put_nowait`, lọc kênh/tuổi, chống trùng tin/tin sửa trong RAM.
- `ram_backpressure.py`: queue có giới hạn, ưu tiên và thống kê bỏ tin khi quá tải.
- `main_script.py`: đăng ký handler **trước** `client.start()`; log gộp thay vì log INFO mỗi tin, worker xử lý tách riêng.

Telethon có thể tự gửi RPC nội bộ để đồng bộ **trạng thái cập nhật** sau mất mạng; bản 4 tầng cũng có vòng `GetChannelDifference` chủ động trên kênh ưu tiên cao (mặc định priority ≤ 6), **không** gọi GetHistory. Khi socket đứt, các tin Telegram phát lúc mất kết nối **không được đảm bảo** bot sẽ xử lý lại, bởi cấu hình live-only không quét bù lịch sử. Hàng đợi RAM cũng mất khi dừng chương trình.

## Cài đặt trên Windows

1. Dừng bot cũ (`Ctrl+C`), sao lưu thư mục cũ.
2. Giải nén bản này vào thư mục mới, giữ cấu trúc tệp.
3. **Tự sao chép** `.env`, thư mục `data/`, session Telethon và cấu hình trình duyệt từ bot cũ vào thư mục mới. Những tệp riêng tư này không nằm trong bản ZIP mới.
4. Chạy `setup.bat` nếu chưa cài thư viện. Chạy `run.bat` và hoàn tất đăng nhập Telegram nếu được yêu cầu.
5. Kiểm tra log có `MTProto TCP PUSH SOCKET`, `handlers registered before socket connect`, `connected=True`; theo dõi `[Backpressure]` và `[Telegram admission]` để đánh giá tình trạng quá tải.

Điều chỉnh bằng `.env`:

```env
INGEST_QUEUE_MAXSIZE=128
MESSAGE_WORKERS=6
OCR_MEDIA_QUEUE_MAXSIZE=12
DOMAIN_QUEUE_MAXSIZE=64
TELEGRAM_RAW_DIAGNOSTICS=false
TELEGRAM_INGRESS_AGE_DROP_ENABLED=true
```

Khi quá tải: bot **ưu tiên phản hồi nhanh**, không bảo đảm giữ toàn bộ tin. Để không mất tin, cần thiết kế hàng đợi bền vững và replay/reconciliation có kiểm soát, không thể vừa cấm lưu/replay vừa bảo đảm nhận đủ tin qua mọi lần mất mạng.

Kiểm tra offline: `python -m unittest discover -s tests -v`. Test này không cần kết nối Telegram.

## Dọn các hàm không còn dùng (07/11/2026)

Đã kiểm tra các lời gọi trực tiếp, `getattr`/tên hàm dưới dạng chuỗi, API re-export,
callback của Telethon, Playwright, class override và các file `.bat` trước khi dọn.
Chỉ loại bỏ các hàm không còn đường gọi trong bản bot này:

| Tệp | Hàm loại bỏ | Lý do |
|---|---|---|
| `main_script.py` | `process_telegram_message` | Bộ nhận kiểu cũ, không được đăng ký; nay dùng `MTProtoPushSocket` |
| `main_script.py` | `_ocr_is_image_message` | Không còn nơi gọi |
| `main_script.py` | `_ocr_has_known_code_input_link` | Không còn nơi gọi |
| `scheduling.py` | `freshest_first_enabled` | Hàm chế độ cũ, không còn nơi gọi |
| `tg_ingest.py` | `TelegramReceiver.on_push` | Wrapper async cũ; socket gọi `offer_nowait` trực tiếp |
| `disk_guard.py` | `ensure_disk_space` | Không còn nơi gọi; hoạt động dọn định kỳ khác được giữ nguyên |
| `sc88_captcha.py` | `recognize_sc88_captcha` | Module độc lập không được import hoặc chạy (xóa cả file) |

Các import chỉ phục vụ phần mã đã xóa cũng được dọn. **Không xóa** các phương thức
được gọi qua tên chuỗi (`getattr`), các callback khi nhận sự kiện, method override,
hoặc chức năng bảo vệ submit, tải media, OCR, queue, khóa tài khoản và lưu DB.

Dọn mã chết chủ yếu giúp dự án dễ bảo trì; không tự làm tốc độ MTProto/socket
nhanh hơn. Để kiểm thử trên máy của bạn, chạy `python -m unittest discover -s tests -v`.

## Bản 4 tầng MTProto — Prewarm / Push+Raw+Difference / Admission / Worker (07/11/2026)

Bot được cập nhật trên bản MTProto đã dọn hàm thừa, **không đổi** HTTP submit,
Playwright, khóa tài khoản, trình tải/OCR, cấu hình website hoặc lịch sử kết quả.

```text
Telegram MTProto TCP — Telethon
  ├─ events.NewMessage / MessageEdited (không await công việc khác)
  ├─ events.Raw(UpdateNewChannelMessage / UpdateEditChannelMessage)
  └─ Active GetChannelDifference [chỉ kênh priority <= 6, giới hạn RPC]
       ▲ InputPeerChannel(access_hash) + pts làm nóng từ get_dialogs(500)
       │ chỉ gọi GetFullChannel khi thiếu PTS; tối đa 120s startup
       ▼
 Lọc enabled, giờ mở cửa, chống trùng TTL 600s, tuổi 40/45/120s
       ▼
 Bounded RAM ingress; spoiler → text → image → video
       │ trong cùng loại, priority thấp hơn được phục vụ trước
       ├─ 6 message workers (16 tác vụ xử lý đồng thời toàn cục)
       ├─ Fast text/spoiler → trích mã
       └─ image/video → OCR queue riêng → giới hạn worker
       ▼
 Bounded domain queue → khóa tài khoản/tab → HTTP/Playwright submit
```

### Cấu hình `.env`

```dotenv
TELEGRAM_SYNC_DIALOGS=true
TELEGRAM_DIALOG_SYNC_LIMIT=500
TELEGRAM_DIALOG_SYNC_TIMEOUT=120.0
TELEGRAM_CHANNEL_TIMEOUT=10.0
TELEGRAM_ACTIVE_CHANNEL_DIFFERENCE=true
TELEGRAM_DIFF_PRIORITY_MAX=6
TELEGRAM_DIFF_INTERVAL_SEC=6.0
TELEGRAM_DIFF_CONCURRENCY=1
TELEGRAM_INGRESS_DEDUPE_TTL_SEC=600.0
INGRESS_XX88_MAX_AGE_SEC=40.0
INGRESS_OCR_MAX_AGE_SEC=45.0
INGRESS_MAX_AGE_SEC=120.0
INGEST_QUEUE_MAXSIZE=128
MESSAGE_WORKERS=6
MAX_CONCURRENT_PROCESSING=16
```

`TELEGRAM_DIFF_CHAT_IDS` cho phép chọn chính xác các kênh đồng bộ dự phòng;
khi để trống mới dùng ngưỡng `TELEGRAM_DIFF_PRIORITY_MAX`. Bản cấu hình hiện tại
chọn 10 kênh: XX8, XX88 chính và esport, ba kênh LLwin, V79, E39 và hai kênh SC88.
Các kênh còn lại dùng Push/Raw. `TELEGRAM_DIFF_MAX_CHANNELS` luôn giới hạn tối đa 10.
Khoảng cách RPC toàn cục tối thiểu 0,5 giây; chu kỳ mỗi kênh ít nhất 6 giây theo
cấu hình hiện tại và chờ lâu hơn nếu Telegram trả `timeout`. FloodWait tạm dừng
RPC dự phòng của tất cả các kênh, không chặn handler Push/Raw. Có thể tắt bằng
`TELEGRAM_ACTIVE_CHANNEL_DIFFERENCE=false` để trở về Push/Raw-only.

Nếu mất kết nối, Telethon tự phục hồi state; không gọi `GetHistory` và không
bắt buộc chạy `catch_up` tại startup. `GetChannelDifference` chỉ cố gắng đối
chiếu tin gần thời gian chạy từ PTS hiện có, không phải cơ chế đảm bảo thu đủ
mọi tin. Timeout RPC tối đa 10s, concurrency 1, interval tối thiểu 5s,
backoff khi lỗi/FloodWait, tối đa 3 trang mỗi đợt. Đặc biệt tránh chạy nhiều
bản bot cùng một session Telegram vì dễ trùng RPC/khóa SQLite.

Không thể cam kết cửa lọc luôn dưới 1ms hay loại bỏ hoàn toàn độ trễ push nếu
không benchmark mạng/DC thật và máy Windows. Dưới tải quá lớn, RAM queue có
thể chủ động bỏ tin ưu tiên thấp, và 600s TTL được giới hạn 10.000 khóa.

Không bảo đảm chống trùng vĩnh viễn: cache nhận tin giữ tối đa 10.000 phiên bản
trong 600 giây; tin sửa có nội dung mới vẫn được xử lý. Các lớp bảo vệ gửi lại
kết quả chưa rõ vẫn giữ nguyên. Dữ liệu tin không ghi vào SQLite inbox.
DB kết quả được dọn nền mỗi 300 giây: giữ 30 ngày, tối đa 20.000 dòng mỗi bảng
chi tiết và 100.000 dòng mỗi bảng guard; file WAL được checkpoint định kỳ.
Lịch sử CSV/JSONL quá 30 ngày được dọn cùng tác vụ nền; marker kết quả chưa rõ
trong RAM chỉ giữ ngày hiện tại. Vì vậy dữ liệu mới không cần là tin trùng
thì vẫn được kiểm soát kích thước.

**Tự kiểm tra:** `python -m unittest discover -s tests -v`; kiểm tra log
`[MTProtoPrewarm]`, `[MTProtoDiff]`, `[Telegram admission]`, `[Backpressure]`.
