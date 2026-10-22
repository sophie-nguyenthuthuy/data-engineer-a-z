# 44 câu hỏi phỏng vấn Data Engineer — gợi ý trả lời ngắn

Gợi ý là **khung** 3–5 dòng, không phải đáp án duy nhất. Trả lời hay = khung này + một chuyện thật bạn gặp trong lab
(con số, tên file, lỗi bạn đã sửa). Người phỏng vấn nhớ chuyện, không nhớ định nghĩa.

---

## Phần A — 24 câu từ 24 video

### #00 — DE vs DA vs DS; ai chịu trách nhiệm khi dashboard sai
- DE: đưa dữ liệu **đúng, đúng giờ, đúng dạng** tới nơi cần — pipeline, lưu trữ, chất lượng. DA: biến dữ liệu thành câu trả lời nghiệp vụ. DS: mô hình/dự báo.
- Dashboard sai số: DE own **tính đúng của dữ liệu đầu vào** (dedup, tz, thiếu partition); DA own **logic tính** (filter, join, định nghĩa metric). Phân định bằng contract/định nghĩa metric ghi rõ, không bằng cãi nhau.
- Câu trả lời tốt nêu luôn cơ chế: quality check ở silver (DE) + test metric ở gold (DA) để lần sau biết ngay ai sửa.

### #01 — Sáng mùng 1 Tết, doanh thu 30 Tết thiếu 7 triệu
- DE kiểm đầu tiên: pipeline đêm qua có chạy đủ partition `dt=2026-02-16`? rows_in/rows_out, quarantine có tăng bất thường (null_store, tz_drift đẩy hoá đơn sang ngày khác)?
- DA: định nghĩa "doanh thu" (có trừ discount? kênh app tính ở đâu?), filter ngày theo giờ VN hay UTC.
- DS: không liên quan tới số thực tế — chỉ xem forecast có bị ảnh hưởng. Owner con số: DE own dữ liệu đúng, DA own cách tính; **một người** được chỉ định trả lời sếp (thường DA), nhưng root cause tz_drift thì DE sửa.

### #02 — Query thẳng vào POS cho dashboard realtime
- Query plan: dashboard làm `GROUP BY store, date` quét toàn bảng → seq scan + sort/hash agg trên OLTP, lock/IO tranh với máy POS đang INSERT; mỗi refresh 30 s là một full scan.
- Giải thích bằng OLTP vs OLAP: row store, index phục vụ point lookup, không phục vụ aggregate.
- Tạm trong tuần: **read replica** (ít rủi ro, không đổi code) → dashboard query replica, chấp nhận trễ vài giây. CDC là đúng về dài hạn nhưng không xong trong 1 tuần; dump hàng giờ thì "realtime" chết. Nêu điều kiện chuyển: khi replica cũng chậm → lake + Trino/DuckDB.

### #03 — Dedup với ROW_NUMBER khi ts giống nhau
- `ORDER BY ts` với tie → **không deterministic**: hai lần chạy có thể giữ bản khác nhau. Sửa: thêm tie-breaker ổn định (`ORDER BY ts, ingested_at, ctid/offset`), hoặc chọn bản có nhiều thông tin hơn (`store_id IS NULL` xếp sau — repo làm vậy ở silver).
- Nếu bản sau là bản sửa: cần cột **version/updated_at** ở nguồn (hoặc LSN từ CDC) rồi `ORDER BY updated_at DESC`. Không có cột đó → không thể biết bản nào đúng; đây là yêu cầu đẩy ngược về team nguồn qua contract.

### #04 — pandas bị kill với CSV 12 GB
- Rẻ hơn Spark: (1) **Polars lazy** / **DuckDB** đọc streaming, chỉ cột cần, predicate pushdown — 12 GB CSV trên 16 GB RAM chạy được nếu kết quả nhỏ; (2) chuyển CSV → Parquet một lần rồi mọi query sau rẻ; (3) chunking pandas nếu phải giữ pandas API.
- Tiêu chí: dữ liệu vừa **một máy** (< vài trăm GB) → DuckDB/Polars; cần SQL cho người khác → DuckDB; cần API dataframe + lazy → Polars; chunking chỉ khi logic không nhóm được (ít khi).
- Spark khi: dữ liệu nhiều máy, hoặc đã có cluster/team vận hành, hoặc cần streaming thống nhất.

### #05 — `connection refused` 1/10 lần sau deploy
- Nghi: race khi khởi động — Postgres container "started" nhưng chưa accept connection; `depends_on` không có `condition: service_healthy`, hoặc healthcheck chỉ `pg_isready` mà DB init chưa xong.
- Kiểm: `docker compose ps` xem health, `docker compose logs postgres --tail 50` thời điểm deploy, so timestamp với log task lỗi; `docker compose config` xem depends_on thật sự có condition không.
- Sửa cả 3 lớp nhưng đúng chỗ: healthcheck đúng nghĩa (kết nối + query), depends_on dùng healthy, **và** retry/backoff trong code vì production (K8s) không có depends_on — pod bị reschedule bất cứ lúc nào.

### #06 — Tăng giá Bạc xỉu 39k → 45k, margin tháng 2 có đổi?
- Nếu `dim_menu_item` là **SCD type 1** (ghi đè giá) và fact chỉ lưu `item_id` → báo cáo tháng 2 join giá mới → margin tháng 2 **đổi sai**. Nếu fact lưu `unit_price_vnd` tại thời điểm bán (repo làm vậy ở `fct_sales`) → không đổi, đúng.
- "Margin tháng 2 theo giá mới" (what-if) cần giá hiện hành: type 1 trả lời được câu này nhưng mất lịch sử; **type 2** (`menu_price_snapshot` với `valid_from/to`) trả lời được cả hai: join theo thời điểm bán cho số thật, join bản `is_current` cho what-if.
- Giá phải trả: type 1 — mất lịch sử, báo cáo cũ trôi; type 2 — join phức tạp hơn (`between valid_from and valid_to`), dim phình, phải chạy snapshot đều; lưu giá vào fact — fact to hơn, đổi giá hồi tố không sửa được, nhưng đơn giản và đúng cho kế toán. Thực tế hay dùng **cả hai**: giá trong fact (sự thật lúc bán) + dim type 2 (phân tích).

### #07 — Chạy lại lúc 3h, không đếm hai lần
- Idempotency theo **partition**: mỗi run ghi đè toàn bộ `dt=<ds>` (write-then-rename/overwrite), không append. Chạy lại = cùng kết quả. Test: chạy 2 lần, đếm bằng nhau (`test_end_to_end_local_lake`).
- Thêm dedup theo khoá nghiệp vụ trong silver phòng trùng từ nguồn (retry POS).
- Nguồn Postgres thay đổi liên tục: lần chạy 3h thấy **nhiều dữ liệu hơn** lần 2h (hoá đơn sửa, thêm muộn) → partition khác nội dung nhưng vẫn đúng tại thời điểm chạy. Cần ranh giới ổn định: snapshot theo `loaded_at <= cutoff`, hoặc CDC + watermark. Và bronze giữ cả hai bản để truy vết.

### #08 — 50 GB Parquet mà chậm
- Footer: số row group, **row group size**, min/max statistics từng cột, encoding/compression, có dictionary không, schema (string thay cho int/date?).
- 3 nguyên nhân hay gặp: (1) **file nhỏ** (hàng chục nghìn file vài MB — overhead mở file > đọc); (2) không có partition/sort theo cột filter → stats vô dụng, quét hết; (3) kiểu dữ liệu sai (ts dạng string, số dạng string) → không pushdown, phải cast mỗi query. Thêm: compression nặng (gzip) cho workload đọc nhiều.

### #09 — Bug silver 6 tháng trước
- Vì bronze là **raw bất biến** và silver idempotent theo partition: sửa code → chạy backfill 180 partition (DAG `coffee_backfill`, params start/end) → dbt build lại gold. Thời gian = 180 × thời gian một ngày, chạy song song được nếu storage chịu.
- Thiết kế làm được: tách bronze/silver, partition theo ngày, pipeline nhận `--date`, không `datetime.now()`.
- Bronze bị lifecycle xoá sau 90 ngày → chỉ sửa được 90 ngày gần; 90 ngày còn lại phải lấy lại từ nguồn (nếu POS còn giữ) hoặc chấp nhận. Bài học: retention bronze phải ≥ khoảng thời gian bạn dám cam kết re-process; rẻ hơn nhiều so với mất dữ liệu (xem cost model #23).

### #10 — 199 task 10 s, 1 task 20 phút
- Mở **Stages → stage đang chạy → Summary Metrics / Tasks**: nhìn cột *Shuffle Read Size / Records* và *Duration* của task chậm so với median. Lệch hàng chục lần = **data skew**.
- Nguyên nhân: một key chiếm phần lớn dữ liệu (store lớn, `NULL`, giá trị mặc định), hoặc partition đầu vào không đều (một file khổng lồ).
- Nói thêm được: SQL tab xem plan có SortMergeJoin/Exchange ở đâu; Executors tab xem GC/spill.

### #11 — Join 2 TB với 50 GB, 3 % NULL
- Mọi `customer_id = NULL` hash về **một partition** → một task nhận 3 % × 2 TB = 60 GB → treo. (NULL không join được với NULL nhưng Spark vẫn phải shuffle chúng.)
- Sửa: (1) tách NULL ra trước join (`filter isNotNull`) rồi union lại với cột khách = null — nhanh nhất; (2) **salting** key skew; (3) AQE `skewJoin` (Spark 3) tự chia partition lớn.
- Broadcast: 50 GB **quá lớn** để broadcast (giới hạn thực tế vài trăm MB – vài GB); chỉ được nếu lọc bảng khách còn tập nhỏ liên quan trước. Nêu được ngưỡng `spark.sql.autoBroadcastJoinThreshold` là điểm cộng.

### #12 — fct_sales incremental
- `materialized='incremental'`, filter `where business_date >= (select max(business_date) - interval 'N days' from {{ this }})` với lookback N ngày cho late data; `unique_key='line_id'` (grain line item) với `incremental_strategy='merge'`/`delete+insert`.
- Silver ngày cũ re-process: nếu ngày đó ngoài lookback → không được cập nhật → cần **full refresh** hoặc chạy incremental với var ngày cụ thể (partition overwrite theo `business_date` là chiến lược sạch nhất).
- Vẫn full refresh khi: đổi logic model, đổi schema, dữ liệu nhỏ (40 phút → nếu tối ưu còn 5 phút thì incremental thêm phức tạp không đáng).

### #13 — 3 partition, 5 consumer
- Mỗi partition chỉ được **một** consumer trong group → 2 consumer ngồi không; throughput không tăng. Muốn nhanh: tăng partition (≥ số consumer) hoặc tối ưu xử lý trong consumer (batch, async I/O).
- Tăng partition về sau: hash(key) → partition **thay đổi**, message cùng `store_id` trước/sau thời điểm đổi rơi vào partition khác → **mất thứ tự** cho key đó trong lúc chuyển; consumer stateful (window theo store) có thể sai. Kế hoạch: tạo topic mới với số partition đủ, migrate, hoặc chọn số partition dư từ đầu.

### #14 — "Phải exactly-once"
- Idempotent producer + transactions chỉ cho exactly-once **trong Kafka** (producer → topic → consumer-transform → topic). Ghi ra Postgres là **sink ngoài** → Kafka transaction không bao phủ.
- Phần thiếu: sink idempotent — `INSERT ... ON CONFLICT (transaction_id) DO UPDATE` (upsert theo khoá nghiệp vụ), hoặc ghi offset **cùng transaction DB** với dữ liệu (lưu offset vào bảng, khi restart đọc offset từ DB). Effectively-once ở đầu cuối = at-least-once + idempotent sink.
- Thêm DLQ cho bản ghi không parse/không upsert được; schema registry để không nổ vì schema đổi.

### #15 — Watermark 10 phút
- Watermark không phải độ trễ dashboard: output trễ phụ thuộc **trigger** và **output mode**. Với `update` mode, kết quả ra mỗi trigger (vài giây), watermark chỉ quyết định **khi nào đóng window / bỏ state**. Nếu đang dùng `append` mode thì mới trễ đủ watermark.
- Nếu buộc giảm xuống 30 s: event tới muộn hơn bị **drop** khỏi aggregate streaming → thiếu doanh thu (~1 % late_event trong data này). Bù: ghi raw event vào lake (bronze) và batch nightly tính lại số chính thức — streaming cho "gần đúng ngay", batch cho "đúng hẳn". Đo tỉ lệ late để chọn watermark bằng số liệu thay vì cảm tính.

### #16 — Debezium chết 7 tiếng
- Replication slot vẫn giữ WAL vì consumer chưa confirm → **WAL phình trên đĩa Postgres POS**; 7 tiếng ở chuỗi 30 cửa hàng chưa đầy đĩa nhưng ở hệ lớn có thể làm OLTP ngừng. Dữ liệu không mất — khi connector lên lại nó đọc tiếp từ LSN đã confirm.
- Query: `SELECT slot_name, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) FROM pg_replication_slots;` — xem `active = false` và lag.
- Thiết kế lại: alert trên **lag của slot** (metric Postgres) và trên `connector state != RUNNING` (Connect REST API) với tự restart; `max_slot_wal_keep_size` để Postgres tự bảo vệ (chấp nhận mất slot còn hơn chết DB); freshness check ở lake (#21) là lớp cuối.

### #17 — Parquet lake → Iceberg
- 3 vấn đề Iceberg giải cho team có lake Parquet 2 năm: (1) **đổi schema/partition** không cần rewrite toàn bộ (partition evolution, hidden partitioning); (2) **ghi atomic + nhiều writer** không còn thấy file dở (snapshot isolation) — hết cảnh dashboard đọc partition đang ghi; (3) **time travel / rollback** khi silver sai (so với hiện tại phải backfill từ bronze), và row-level delete/update (GDPR, sửa hoá đơn).
- 2 chi phí mới: **catalog** (REST/Glue/Hive) là thành phần phải vận hành và là điểm single-of-failure mới; **maintenance jobs** — compaction file nhỏ, `expire_snapshots`, `remove_orphan_files` — không chạy thì storage và metadata phình, query chậm hơn Parquet thuần.
- Trả lời hay nêu cả "chưa cần": nếu 1 writer, append-only theo ngày, không sửa quá khứ → Parquet + Hive partition đang đủ.

### #18 — "Dashboard real-time" cho 30 cửa hàng
- Hỏi lại: real-time để làm quyết định gì? Phần lớn là "trong giờ" là đủ. Đề xuất **lambda tối giản**: streaming chỉ cho 1–2 metric vận hành (đơn/phút, máy pha lỗi — IoT), còn doanh thu chính thức đi batch hằng ngày (#19) vì hoá đơn có sửa/huỷ.
- Kiên quyết không streaming: báo cáo tài chính, đối soát, bất kỳ số nào cần "đúng hẳn" — stream là gần đúng (late event, dedup khó).
- Đo sau go-live: (1) **freshness thật** của từng dashboard vs người dùng thực sự mở lúc nào (nếu họ chỉ xem 8h sáng thì streaming vô ích); (2) chi phí/tháng streaming vs batch; (3) số incident do streaming. Có số rồi mới quyết tiếp.

### #19 — Fail 02:00, fix 06:00
- Clear task fail → Airflow chạy lại **cùng run** với `ds` cũ (ngày hôm trước, vì run 02:00 ngày D+1 có data interval = ngày D). Task idempotent ghi đè partition `dt=ds` → không trùng.
- `{{ ds }}` lúc 06:00 vẫn là ngày D — vì nó gắn với run, không với giờ chạy. Nếu DAG dùng `datetime.now()` → lúc 06:00 nạp ngày D+1 (thiếu ngày D, D+1 chưa đủ dữ liệu), và backfill vô nghĩa hoàn toàn. Test `ast` trong lab #19 bắt lỗi này.

### #20 — Size XL xuất hiện
- Phát hiện đầu tiên ở **contract silver** (`allowed: [S, M, L]`) — rẻ nhất và sớm nhất, trước dbt. dbt `accepted_values` ở gold bắt được nhưng muộn hơn và chỉ nếu ai đó viết test. Dashboard thì không bao giờ "bắt" — chỉ hiển thị sai hoặc thiếu.
- Cứng hay mềm: giá trị **mới nhưng hợp lệ nghiệp vụ** (XL) → fail mềm với cảnh báo + đưa dòng vào quarantine/giữ nguyên tuỳ rủi ro, không dừng doanh thu cả chuỗi vì một size. Lỗi **cấu trúc** (cột mất, kiểu sai, dòng = 0) → fail cứng. Nguyên tắc: chặn cái làm hỏng downstream, cảnh báo cái chỉ làm thiếu một lát cắt. Và cập nhật contract có version + review từ team POS.

### #21 — Thiếu 3 cửa hàng trong 10 phút
- Thứ tự: (1) **metric** rows_out theo bảng cho `dt` hôm qua vs 7 ngày trước — thấy ngay thiếu bao nhiêu, bảng nào; (2) **Airflow** run hôm qua: task nào retry/fail, log `ingest_bronze_silver` có warning; (3) **Marquez**: dataset silver có được cập nhật không, run nào tạo ra nó, upstream (Postgres) có bất thường (rowCount facet); (4) quarantine: 3 cửa hàng có `store_id` null hay mã mới không khớp pattern?
- Metric nên báo trước: **số store_id distinct** trong silver so với dim_store (27 vs 30), hoặc quarantine ratio tăng. rows_out tổng không đủ nhạy (3/30 cửa hàng nhỏ chỉ giảm vài %).

### #22 — CI cho PR đổi model dbt
- Tối thiểu không cần warehouse: `dbt parse` (Jinja, ref sai, YAML), `sqlfluff`/ruff, **dbt build trên DuckDB với samples** (repo làm ở job smoke) — chạy thật model + test với dữ liệu nhỏ.
- `dbt parse` bắt: lỗi cú pháp Jinja, ref/source không tồn tại, config sai. Bỏ sót: **SQL sai** (cột không tồn tại, kiểu không khớp), logic sai, test fail — vì không chạy query.
- Thêm 15 phút: `dbt build --select state:modified+` trên dữ liệu mẫu (Slim CI), so sánh row count/giá trị tổng của model đổi với `main` (data diff), `dbt docs generate` kiểm tra model mới có description.

### #23 — Marketing xin export số điện thoại
- Không gửi file. Đưa **danh sách customer_id (tokenized)** vào hệ thống gửi SMS có quyền đọc phone từ nguồn chính thức, qua kênh có log (ví dụ job trong Airflow ghi ai yêu cầu, bao nhiêu dòng, mục đích, thời điểm). Nếu phải export: cột tối thiểu, mã hoá file, hết hạn, ghi access log, có approver.
- "Hash là đủ ẩn danh": sai — hash không salt của số điện thoại brute-force được (7×10⁸ giá trị), và hash ổn định cho phép join chéo. Hash/tokenize là **pseudonymization**, vẫn là dữ liệu cá nhân theo Nghị định 13/2023; chỉ giảm rủi ro, không miễn trách nhiệm.

### #24 — Dự án tự hào nhất
- Kể theo Problem → Architecture → Quality → Lessons (khung README), 2 phút.
- Chọn **một con số** bạn tự đo (ví dụ "cách ly 1.5 % bản ghi lỗi" hay "giảm 88 % dung lượng"), nói rõ lệnh/test tạo ra nó, và **mở laptop chạy lại** hoặc chỉ vào CI/log public. Nếu nghi ngờ → "đây là test trong repo, đây là output CI" — đó là lý do mọi số trong README phải tái tạo được.

---

## Phần B — 20 câu phổ biến

### SQL
**B1.** `WHERE` lọc **dòng trước** khi nhóm; `HAVING` lọc **nhóm sau** aggregate.
```sql
select store_id, avg(revenue_vnd) as avg_daily
from fct_daily_store_revenue
where business_date >= '2026-02-01' and business_date < '2026-03-01'
group by store_id
having avg(revenue_vnd) > 3_000_000;
```

**B2.** `ROW_NUMBER` đánh 1,2,3 không trùng; `RANK` trùng hạng rồi nhảy (1,1,3); `DENSE_RANK` trùng không nhảy (1,1,2). Dedup dùng **ROW_NUMBER** + tie-breaker ổn định, lấy `= 1` — vì cần đúng một dòng.

**B3.** Nhìn: node tốn thời gian nhất (actual time), **Seq Scan vs Index Scan**, rows estimated vs actual (lệch lớn → statistics cũ, `ANALYZE`), join type (Nested Loop với bảng lớn là dấu đỏ), Sort/Hash spill ra disk. Index không giúp khi: lọc trả về phần lớn bảng, cột có cardinality thấp, hàm bọc cột (`date(ts)`), hoặc bottleneck là aggregate/sort chứ không phải lookup.

**B4.** Về nguyên tắc optimizer xử lý CTE/subquery như nhau; Postgres ≥ 12 inline CTE (trừ khi `MATERIALIZED` hoặc dùng > 1 lần). Temp table **ngắt** optimizer: tốt khi muốn ép thứ tự/giữ kết quả trung gian lớn tái dùng, có thể `ANALYZE`; xấu vì ghi disk. Chọn theo dễ đọc trước, đo `EXPLAIN` sau.

**B5.**
```sql
select store_id, business_date, revenue_vnd,
       sum(revenue_vnd) over (partition by store_id order by business_date) as cum_revenue,
       revenue_vnd * 1.0 / lag(revenue_vnd) over (partition by store_id order by business_date) - 1 as pct_change
from fct_daily_store_revenue;
```
Nhắc: ngày không có dòng (cửa hàng đóng) → `lag` nhảy qua ngày; muốn đúng phải join `dim_date`.

### Spark
**B6.** Transformation (map/filter/join) chỉ **xây plan**; action (count/write/collect) mới chạy. Lợi: optimizer gộp, pushdown, bỏ cột thừa. Khó: lỗi báo ở action chứ không ở dòng gây lỗi; `df.count()` để "debug" tạo thêm job đắt; dùng `explain()` và Spark UI SQL tab thay vì count.

**B7.** `repartition(n)` shuffle đầy đủ, tăng/giảm, phân bố đều; `coalesce(n)` chỉ gộp partition hiện có, không shuffle, chỉ giảm, có thể lệch. Sau filter còn 1 %: `coalesce` xuống số file hợp lý (mục tiêu 128 MB–1 GB/file) trước khi ghi Parquet — tránh hàng nghìn file bé.

**B8.** Shuffle = chuyển dữ liệu giữa executor theo key (groupBy, join, repartition, distinct, window). Giảm: (1) **broadcast** bảng nhỏ; (2) filter/project **trước** join; (3) partition/bucket dữ liệu theo key join sẵn ở storage, hoặc tận dụng AQE coalesce partitions; thêm: giảm `spark.sql.shuffle.partitions` cho dữ liệu nhỏ, tránh `distinct` không cần.

**B9.** Lợi khi một DataFrame được dùng **≥ 2 action** và tính lại đắt. Hại: chiếm memory → spill/GC, cache DataFrame chỉ dùng 1 lần, quên `unpersist`. Hay dùng `MEMORY_AND_DISK` (mặc định cho DataFrame); `MEMORY_ONLY` dễ mất block và tính lại.

### Kafka / Streaming
**B10.** Crash sau xử lý, trước commit → restart đọc lại từ offset cũ → xử lý **hai lần** = **at-least-once**. Muốn tránh: sink idempotent (upsert theo khoá) hoặc lưu offset cùng transaction với kết quả. Commit trước xử lý → at-most-once (mất).

**B11.** Event time = lúc việc xảy ra (`ts` khách bấm đặt); processing time = lúc hệ thống nhận (`ingested_at`). Trong repo: `app_events.jsonl` và `iot_readings.csv` có ~1 % dòng `ingested_at − ts` = 1–10 phút (mất mạng, retry) — aggregate theo processing time sẽ dồn doanh thu sang phút sai.

**B12.** Chỉ **trong một partition**. Key = `store_id` → mọi event của cửa hàng vào cùng partition, đúng thứ tự; đánh đổi: skew nếu một cửa hàng rất lớn, và không đổi số partition về sau (câu #13).

### Data modeling
**B13.** Star: fact ở giữa, dim denormalize phẳng → ít join, BI tool tự sinh SQL dễ, người dùng hiểu. Snowflake: dim chuẩn hoá tiếp (dim_store → dim_district → dim_city) → ít trùng, nhiều join. Denormalize thêm khi: dashboard hỏi 1–2 câu cố định với latency thấp (bảng `fct_daily_store_revenue` chính là vậy), hoặc dim nhỏ.

**B14.** Type 1 ghi đè (mất lịch sử); type 2 thêm dòng mới với `valid_from/valid_to` (giữ lịch sử); type 3 thêm cột "giá trước". Bảng giá menu → **type 2**: hoá đơn tháng 1 phải join đúng giá tháng 1. dbt `snapshot` với `strategy='check'` (hoặc `timestamp`) tự tạo `dbt_valid_from/to` mỗi lần chạy — `menu_price_snapshot` trong repo.

**B15.** Grain = **một dòng đại diện cho cái gì**; quyết định trước mọi cột. `fct_sales` (line item) trả lời món nào bán ở đâu, margin theo món; `fct_daily_store_revenue` (ngày × cửa hàng) là **aggregate table** cho dashboard nhanh và ổn định định nghĩa doanh thu. Không trộn grain trong một bảng (sẽ đếm trùng).

**B16.** Natural key `transaction_id` không đủ khi: có **duplicate** từ nguồn (repo này), hệ thống POS mới sinh lại mã trùng với cũ, hoặc gộp nhiều nguồn (POS + app `order_id`). Surrogate key (hash của khoá tự nhiên + nguồn, hoặc sequence) tách gold khỏi thay đổi của nguồn; nhưng vẫn phải giữ natural key để truy vết.

### System design
**B17.** 500 k đơn/ngày ≈ 6 đơn/s trung bình, peak ×10 — **không cần streaming** cho dashboard 15 phút. Nguồn: DB đơn hàng (CDC Debezium → Kafka) + app events (SDK → Kafka/Kinesis) + đối tác giao hàng (API batch). Lưu: lake Parquet/Iceberg partition theo giờ, bronze raw bất biến. Transform: micro-batch 5–15 phút (Spark hoặc dbt trên warehouse), gold aggregate theo 15 phút × khu vực. Phục vụ: warehouse/Trino + BI. Chi phí: vài trăm USD/tháng chủ yếu compute. Hỏng đầu tiên khi ×10: (1) file nhỏ ở lake (micro-batch 5 phút × nhiều partition) → cần compaction; (2) job aggregate skew theo thành phố lớn / giờ cao điểm → cần salting hoặc pre-aggregate.

**B18.** Thay đổi: **đúng tuyệt đối và có thể audit** quan trọng hơn nhanh → batch cuối ngày, reconciliation tổng kiểm (control totals) với core banking, không chấp nhận at-least-once mà không dedup; dữ liệu PII/tài chính → mã hoá at-rest, tokenize, truy cập theo role, lineage bắt buộc (NHNN hỏi số từ đâu); deadline cứng 7h → SLA, retry có giới hạn, **runbook** khi fail và người trực; thay đổi schema qua change management, không "contract mềm". Thường on-prem/private cloud, công cụ theo danh sách được duyệt.

**B19.** Debezium 1 connector/DB (hoặc vài connector theo nhóm bảng), publication chỉ bảng cần, snapshot ban đầu ngoài giờ cao điểm; Kafka → sink lake (Iceberg upsert hoặc Parquet append + merge batch 5 phút). Đo ảnh hưởng OLTP: **replication slot lag / WAL size** (`pg_replication_slots`), IO và CPU của primary trước/sau, latency p99 của query nghiệp vụ; đặt `max_slot_wal_keep_size`. Độ trễ < 5 phút đo end-to-end bằng `now() − source_ts` của dòng mới nhất ở lake.

**B20.** 30 ngày: tuần 1 **không sửa gì** — chạy, đọc log, vẽ lineage tay, đo thời gian từng bước, tìm người dùng đầu ra (cái gì thực sự được dùng?), thêm alert tối thiểu khi fail. Tuần 2: viết test kiểm đầu ra hiện tại (golden output) để sửa không làm hỏng; đặt pipeline vào git/CI nếu chưa. Tuần 3: sửa 1–2 bottleneck lớn nhất theo số đo (thường là full scan/full rebuild → incremental, hoặc file format). Tuần 4: bỏ phần không ai dùng, viết README + runbook, chuyển sang lịch/owner rõ ràng. Nguyên tắc: đo trước, sửa sau, mỗi thay đổi có test.
