# K4-Track02-Day17 — Report cá nhân

Phần phân tích tối đa một trang, không tính output ở phần 5.
Định dạng tham chiếu và phạm vi tính trang: [SUBMISSION.md](../docs/SUBMISSION.md).

**Họ tên / MSSV: Nguyễn Tiến Đạt / 2A202602970**
**Repo: K4-Track02-Day17-Data-Pipeline-Engineering**
**Commit bài nộp: fbf74549564cf474f0b59b1452db9dd4b07cd131**
**AI đã dùng và phạm vi hỗ trợ (hoặc không dùng): Antigravity (Gemini) — hỗ trợ phân tích triệu chứng lỗi, gỡ lỗi môi trường Linux venv/dbt, đối chiếu parity và hoàn thiện báo cáo.**
**Nguồn tham khảo khác (nếu có):**

## 1. Ba lỗi

Mỗi lỗi 4 dòng. Triệu chứng = thứ bạn _thấy_ đầu tiên (check nào fail, số nào lạ,
checksum nào lệch) — không phải cách sửa.

|                               | Lỗi Silver                                                                                                                                                                                                                                                                                                                              | Lỗi late data                                                                                                                                                                                                                                                                                    | Lỗi xoá (CDC)                                                                                                                                                                                                                                                                     |
| ----------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Triệu chứng**               | `make verify` báo `silver_tickets has 24 rows for 12 tickets`. T-91 cập nhật nhiều lần (tạo 08-10, đổi priority 08-14, closed 08-16) dẫn đến có tới 3 dòng trong `silver_tickets` thay vì 1 dòng trạng thái mới nhất (`high / closed / bug`). `test_silver_tickets_one_row_per_ticket` và `test_silver_tickets_latest_state_wins` fail. | `make lateness` đo được P50=0.00, P95=2.90, P99=3.00 ngày trong khi `config.LOOKBACK_DAYS = 0`. `make verify` báo `u05's offline events of 08-12 (arrived 08-15) are counted on 08-12 (got (2, 0), expected (5, 1))` và `LOOKBACK_DAYS covers measured P99 lateness (LOOKBACK_DAYS=0 < 3)` fail. | T-97 đóng ngày 08-12 và bị xoá ngày 08-15, nhưng `make verify` báo `deleted ticket T-97 is a tombstone: is_deleted and no personal data left` fail (vẫn còn nguyên `is_deleted = False` và dữ liệu cá nhân). T-97 vẫn còn tồn tại trong `gold_training_set` và `gold_doc_chunks`. |
| **Nguyên nhân gốc**           | `pipeline/silver.py` dùng `INSERT INTO` (append-only) thay vì `MERGE INTO`, ghi thêm dòng cho mỗi thay đổi/batch thay vì cập nhật theo khóa `ticket_id` và `_lsn`.                                                                                                                                                                      | `pipeline/config.py` đặt `LOOKBACK_DAYS = 0`, daily run chỉ tính ngày hiện tại, bỏ lọt các event đến trễ theo event time từ các ngày trước đó của u05.                                                                                                                                           | `pipeline/staging.py` chỉ lấy `j->'value'->'after'->>'ticket_id'`. Khi `_op = 'd'`, trường `after` là null nên `ticket_id` bị null và bị loại bởi `WHERE ticket_id IS NOT NULL`. Ngoài ra chưa xóa trắng PII khi tombstone.                                                       |
| **Cách sửa** (file, vài dòng) | Trong `pipeline/silver.py`, đổi `INSERT INTO` thành `MERGE INTO silver_tickets` theo `ticket_id`, cập nhật khi `s._lsn > t._lsn`.                                                                                                                                                                                                       | Trong `pipeline/config.py`, cập nhật `LOOKBACK_DAYS = 3` (bằng `ceil(P99)` đo được từ Bronze).                                                                                                                                                                                                   | Trong `pipeline/staging.py`, lấy `ticket_id` bằng `COALESCE(after->>'ticket_id', before->>'ticket_id', key->>'ticket_id')`. Trong `pipeline/silver.py`, khi `_op = 'd'` thì gán `is_deleted = TRUE` và các trường PII (`user_id`, `subject`, `body`) thành `NULL`.                |
| **Khái niệm trên slide**      | Silver — có khoá / Upsert theo LSN, Idempotency (tính luỹ biến).                                                                                                                                                                                                                                                                        | Event time vs Ingestion time, Lateness & Lookback window / Watermark.                                                                                                                                                                                                                            | Debezium CDC envelope (`before`/`after`/`op`), Tombstone / Soft delete, Right to be forgotten (GDPR).                                                                                                                                                                             |

## 2. Các con số

- P99 lateness đo từ Bronze: `3.00` ngày (P50 = `0.00`, P95 = `2.90`, max = `3` ngày) → chọn `LOOKBACK_DAYS = 3` ngày (`ceil(P99)`).
- Checkpoint late-data: `3 passed, 10 deselected`; u05 ngày `2026-08-12` có `5 events, 3 clicks, 1 feedback down`, và `gold_feature_daily` khớp full recompute.
- `submission/checksums.txt`: **PASS** — Gold checksum: `39e115c510ecdf526800eac227158a4f` (fresh build và cả 3 lần re-run đều giống nhau).
- `make parity`: **PARITY** — `silver_tickets` lite/dbt: `3c15dfd43701`; `gold_feature_daily` lite/dbt: `8630e04a61d1`.

## 3. Lựa chọn công cụ / kỹ thuật (mỗi dòng một câu "vì sao")

- MERGE theo khoá cho `silver_tickets`, overwrite-partition cho `gold_feature_daily`: `silver_tickets` là bảng thực thể cần upsert theo khóa để giữ duy nhất trạng thái mới nhất cho mỗi ticket, trong khi `gold_feature_daily` là bảng tổng hợp theo ngày nên ghi đè cả phân vùng (partition) đảm bảo tính idempotent và tự động tích hợp event đến muộn.
- Tombstone thay vì xoá hẳn hàng trong Silver: Giữ lại bản ghi tombstone (`is_deleted = TRUE`, xóa sạch PII) giúp các tầng downstream (Gold, RAG index, training set) nhận biết sự kiện xóa để đồng bộ loại bỏ dữ liệu thay vì bỏ sót do mất dấu hàng đã xóa.
- Snapshot training dựng lại từ Bronze "as of" ngày đó, không sửa snapshot cũ: Đảm bảo tính bất biến và tái lập (reproducibility) trong ML, tránh rò rỉ dữ liệu tương lai (data leakage) vào các tập huấn luyện của ngày quá khứ.
- DuckDB (lite) / dbt (track dbt) cho bài toán cỡ này, chứ không phải Spark: Dữ liệu ở quy mô vừa và nhỏ (vài MB đến vài GB), DuckDB chạy in-process dạng OLAP vectorized cực nhanh, không tốn tài nguyên quản lý cụm phân tán và chi phí hạ tầng như Spark.

## 4. Hai câu hỏi suy ngẫm

1. Snapshot `v2026-08-12`..`v2026-08-14` vẫn chứa văn bản của T-97 (đã bị xoá ngày
   08-15). "Snapshot bất biến" và "quyền được xoá dữ liệu" mâu thuẫn — bạn xử lý thế nào?

Để giải quyết mâu thuẫn giữa tính bất biến của snapshot và quyền được xóa dữ liệu cá nhân theo GDPR, giải pháp kỹ thuật phù hợp nhất trong thực tế là áp dụng Crypto Shredding. Mỗi người dùng hoặc ticket được mã hóa dữ liệu nhạy cảm bằng một khóa mã hóa riêng; khi nhận yêu cầu xóa, hệ thống chỉ cần xóa bỏ khóa giải mã tương ứng thì toàn bộ dữ liệu trong các snapshot quá khứ lập tức trở thành vô nghĩa mà không cần can thiệp chỉnh sửa cấu trúc các tệp dữ liệu bất biến. Trường hợp không dùng mã hóa, cần thực hiện quy trình ghi đè có lưu vết kiểm toán đối với các bản snapshot cũ nhằm xóa trắng nội dung ticket T-97 để tuân thủ pháp lý, đồng thời loại bỏ ngay bản ghi này khỏi RAG index và các chu kỳ huấn luyện mô hình tiếp theo.

2. Regex che được email và số điện thoại, nhưng tên "Nguyễn Văn An" vẫn còn. Bạn sẽ
   đặt chốt PII nào, ở tầng nào, và đo nó ra sao?

Đối với các thông tin định danh như tên người khi regex email và số điện thoại không bắt được, giải pháp là đặt chốt chặn PII bằng mô hình nhận diện thực thể tên (Named Entity Recognition - NER) kết hợp với danh bạ người dùng từ bảng dữ liệu gốc. Chốt chặn này cần được bố trí tại tầng Silver ngay trước khi ghi dữ liệu vào các bảng lưu trữ để ngăn ngừa việc rò rỉ thông tin cá nhân xuống tầng Gold và hệ thống truy xuất RAG. Hiệu quả của chốt chặn được đo lường thông qua các chỉ số Precision, Recall và F1-score trên tập dữ liệu kiểm thử PII định kỳ, kết hợp cùng hoạt động rà soát ngẫu nhiên trên các cột văn bản tự do.

## 5. Output (dán nguyên văn)

```text
$ make verify
=== verify.py — Day 17 pipeline contracts ===
  [OK ] Bronze  every daily batch landed as Parquet (7 days x 3 sources)
  [OK ] Bronze  re-landing a batch is a no-op (append-only, no duplicate file)
  [OK ] Bronze  Bronze keeps the raw truth: Kafka tombstone + redelivered events are still there
  [OK ] Silver  silver_tickets has exactly one row per ticket_id
  [OK ] Silver  T-91 shows its latest state: high / closed / bug
  [OK ] Silver  deleted ticket T-97 is a tombstone: is_deleted and no personal data left
  [OK ] Silver  no email / phone number survives past Bronze
  [OK ] Silver  silver_events has one row per event_id (Kafka redeliveries removed)
  [OK ] Silver  2 malformed events quarantined with a reason; the run did not halt
  [OK ] Gold    gold_feature_daily reconciles with a full recompute from Silver
  [OK ] Gold    u05's offline events of 08-12 (arrived 08-15) are counted on 08-12
  [OK ] Gold    LOOKBACK_DAYS covers measured P99 lateness (p99=3.00 days)
  [OK ] Gold    training set uses point-in-time priority (T-91 created as 'low')
  [OK ] Gold    late feedback creates a NEW snapshot version; the old one is untouched
  [OK ] Gold    latest training snapshot excludes the deleted ticket T-97
  [OK ] Gold    deletes propagate to the RAG index: no chunk of T-97
  [OK ] Gold    gold_doc_chunks: one row per chunk, and a re-run embeds 0 new chunks
  [OK ] Rerun   re-run 2026-08-12 three times -> Gold checksum identical to a fresh build

RESULT: 18/18 checks — ALL PASS
re-run checksums written to submission/checksums.txt

$ make test
..................................                                       [100%]
34 passed in 2.85s

$ make rerun3
# Lab 17 — re-run check for 2026-08-12

run                     gold_feature_daily    gold_training_set     gold_doc_chunks       gold (combined)
fresh build             8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f

RESULT: PASS — 3 re-runs, identical checksums

$ make lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3

$ make dbt
cd dbt_project && DBT_PROFILES_DIR=. /home/datnguyen-nuoa/lab17/K4-Track02-Day17-Data-Pipeline-Engineering/.venv/bin/dbt build --event-time-start 2026-08-10 --event-time-end 2026-08-17
Running with dbt=1.12.5
Registered adapter: duckdb=1.11.0
Found 5 models, 13 data tests, 2 sources, 502 macros, 1 unit test

Concurrency: 1 threads (target='dev')

1 of 19 START sql view model main.stg_events ................................... [RUN]
1 of 19 OK created sql view model main.stg_events .............................. [OK in 0.09s]
2 of 19 START sql view model main.stg_ticket_changes ........................... [RUN]
2 of 19 OK created sql view model main.stg_ticket_changes ...................... [OK in 0.03s]
3 of 19 START sql incremental model main.silver_events ......................... [RUN]
3 of 19 OK created sql incremental model main.silver_events .................... [OK in 0.10s]
4 of 19 START unit_test silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [RUN]
4 of 19 PASS silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [PASS in 0.12s]
8 of 19 START sql incremental model main.silver_tickets ........................ [RUN]
8 of 19 OK created sql incremental model main.silver_tickets ................... [OK in 0.10s]
5 of 19 START test not_null_silver_events_event_id ............................. [RUN]
5 of 19 PASS not_null_silver_events_event_id ................................... [PASS in 0.04s]
6 of 19 START test not_null_silver_events_user_id .............................. [RUN]
6 of 19 PASS not_null_silver_events_user_id .................................... [PASS in 0.02s]
7 of 19 START test unique_silver_events_event_id ............................... [RUN]
7 of 19 PASS unique_silver_events_event_id ..................................... [PASS in 0.03s]
9 of 19 START test accepted_values_silver_tickets_category__bug__billing__other  [RUN]
9 of 19 PASS accepted_values_silver_tickets_category__bug__billing__other ...... [PASS in 0.02s]
10 of 19 START test accepted_values_silver_tickets_priority__low__medium__high . [RUN]
10 of 19 PASS accepted_values_silver_tickets_priority__low__medium__high ....... [PASS in 0.02s]
11 of 19 START test accepted_values_silver_tickets_status__open__pending__closed  [RUN]
11 of 19 PASS accepted_values_silver_tickets_status__open__pending__closed ..... [PASS in 0.02s]
12 of 19 START test not_null_silver_tickets__lsn ............................... [RUN]
12 of 19 PASS not_null_silver_tickets__lsn ..................................... [PASS in 0.02s]
13 of 19 START test not_null_silver_tickets_is_deleted ......................... [RUN]
13 of 19 PASS not_null_silver_tickets_is_deleted ............................... [PASS in 0.02s]
14 of 19 START test not_null_silver_tickets_ticket_id .......................... [RUN]
14 of 19 PASS not_null_silver_tickets_ticket_id ................................ [PASS in 0.02s]
15 of 19 START test unique_silver_tickets_ticket_id ............................ [RUN]
15 of 19 PASS unique_silver_tickets_ticket_id .................................. [PASS in 0.02s]
16 of 19 START sql microbatch model main.gold_feature_daily .................... [RUN]
Batch 1 of 7 START batch 2026-08-10 of main.gold_feature_daily ....................... [RUN]
Batch 1 of 7 OK created batch 2026-08-10 of main.gold_feature_daily .................. [OK in 0.03s]
Batch 2 of 7 START batch 2026-08-11 of main.gold_feature_daily ....................... [RUN]
Batch 2 of 7 OK created batch 2026-08-11 of main.gold_feature_daily .................. [OK in 0.06s]
Batch 3 of 7 START batch 2026-08-12 of main.gold_feature_daily ....................... [RUN]
Batch 3 of 7 OK created batch 2026-08-12 of main.gold_feature_daily .................. [OK in 0.03s]
Batch 4 of 7 START batch 2026-08-13 of main.gold_feature_daily ....................... [RUN]
Batch 4 of 7 OK created batch 2026-08-13 of main.gold_feature_daily .................. [OK in 0.04s]
Batch 5 of 7 START batch 2026-08-14 of main.gold_feature_daily ....................... [RUN]
Batch 5 of 7 OK created batch 2026-08-14 of main.gold_feature_daily .................. [OK in 0.06s]
Batch 6 of 7 START batch 2026-08-15 of main.gold_feature_daily ....................... [RUN]
Batch 6 of 7 OK created batch 2026-08-15 of main.gold_feature_daily .................. [OK in 0.05s]
Batch 7 of 7 START batch 2026-08-16 of main.gold_feature_daily ....................... [RUN]
Batch 7 of 7 OK created batch 2026-08-16 of main.gold_feature_daily .................. [OK in 0.05s]
16 of 19 OK created sql microbatch model main.gold_feature_daily ............... [SUCCESS in 0.36s]
17 of 19 START test dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [RUN]
17 of 19 PASS dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [PASS in 0.02s]
18 of 19 START test not_null_gold_feature_daily_event_date ..................... [RUN]
18 of 19 PASS not_null_gold_feature_daily_event_date ........................... [PASS in 0.03s]
19 of 19 START test not_null_gold_feature_daily_user_id ........................ [RUN]
19 of 19 PASS not_null_gold_feature_daily_user_id .............................. [PASS in 0.02s]

Finished running 3 incremental models, 13 data tests, 1 unit test, 2 view models in 0 hours 0 minutes and 1.30 seconds (1.30s).
Completed successfully
Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19

$ make parity
=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree
```

Nếu dùng PowerShell, ghi lệnh tương đương và output thực tế theo [SUBMISSION.md](../docs/SUBMISSION.md).
Nếu làm bonus, thêm output B1 hoặc đường dẫn bằng chứng B2 ở cuối phần này.
