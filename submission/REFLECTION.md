# REFLECTION — Top 5 Lakehouse Anti-Patterns

**Anti-pattern team mình dễ vướng nhất: "Small-File Problem" (quá nhiều file nhỏ).**

Team mình build LLM-observability lakehouse ở quy mô ~1 tỷ request/ngày
(xem `bonus/ARCHITECTURE.md`). Telemetry kiểu này đến theo stream, ghi bằng
micro-batch mỗi ~1 phút — mỗi batch đẻ ra một file Parquet tí hon. Sau một ngày
là hàng chục nghìn file < 1 MB trên mỗi partition `(date, tenant_id)`. Đây đúng
là cái bẫy NB2 tái hiện: 200 file nhỏ làm query chậm vì engine phải mở từng
file, đọc footer, và prune min/max kém hiệu quả.

Vì sao rủi ro nhất với *team mình* (chứ không phải schema-drift hay thiếu ACID):
pipeline bản chất là *streaming*, nên small-files là sản phẩm phụ tự nhiên, tích
lũy âm thầm cho tới khi dashboard 5-phút bị trễ. Schema đã enforce ở Bronze
(NB1); ACID thì Delta `_delta_log` lo.

Phòng tránh: lịch `OPTIMIZE` + Z-order theo `tenant_id` định kỳ (NB2 cho 9.3×
speedup, 200→55 file) cộng auto-compaction sau mỗi cửa sổ ghi. Bài học:
small-files không "sai" lúc ghi — nó âm thầm bào mòn hiệu năng đọc.
