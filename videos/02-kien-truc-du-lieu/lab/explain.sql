-- Lab #02: đọc query plan của Postgres (OLTP, row store) cho 2 kiểu câu hỏi.
-- Chạy: docker compose exec -T postgres psql -U coffee -d coffee_pos < videos/02-kien-truc-du-lieu/lab/explain.sql
\timing on

-- Bảng to bao nhiêu trên đĩa? (heap + index)
SELECT pg_size_pretty(pg_total_relation_size('pos_transactions')) AS total_size,
       pg_size_pretty(pg_relation_size('pos_transactions'))       AS heap_only;

-- ── Kiểu 1: câu hỏi OLTP — tra 1 hoá đơn theo khoá ─────────────────────────────
-- Đây là việc Postgres sinh ra để làm. Nhưng pos_transactions KHÔNG có index trên transaction_id
-- (bảng raw cố ý để vậy, xem data/samples/sql/01_schema.sql). Xem plan trước khi có index:
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM pos_transactions WHERE transaction_id = 'HN01-20260215-00010';

-- TODO 1: tạo index trên transaction_id (tên idx_pos_transactions_txn), chạy lại EXPLAIN ở trên.
--   Node đầu tiên đổi từ "Seq Scan" thành gì? "Buffers: shared hit" giảm từ bao nhiêu xuống bao nhiêu?


-- ── Kiểu 2: câu hỏi OLAP — doanh thu theo ngày × cửa hàng ───────────────────────
-- TODO 2: viết EXPLAIN (ANALYZE, BUFFERS) cho câu hỏi:
--   business_date = (ts AT TIME ZONE 'Asia/Ho_Chi_Minh')::date, store_id, sum(total_amount_vnd)
--   from pos_transactions where store_id is not null group by 1, 2 order by 1, 2
--   Câu hỏi: index idx_pos_transactions_store có được dùng không? Vì sao không?


-- TODO 3 (suy nghĩ, không cần chạy): bảng có 10 cột, câu hỏi dùng 3. Với heap (row store) Postgres
--   vẫn phải đọc đủ "Buffers: shared hit=N" page chứa đủ 10 cột. Nếu bảng 500 k dòng và 50 cột
--   (bảng thật của một chuỗi F&B) thì cách lưu theo cột tiết kiệm bao nhiêu I/O? Ghi câu trả lời
--   vào comment video.

-- Dọn: trả bảng về trạng thái ban đầu để các lab sau không bị ảnh hưởng.
DROP INDEX IF EXISTS idx_pos_transactions_txn;
