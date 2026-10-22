-- Lời giải #02: đọc query plan của Postgres (OLTP, row store).
-- Chạy: docker compose exec -T postgres psql -U coffee -d coffee_pos < videos/02-kien-truc-du-lieu/lab/solution/explain.sql
\timing on

SELECT pg_size_pretty(pg_total_relation_size('pos_transactions')) AS total_size,
       pg_size_pretty(pg_relation_size('pos_transactions'))       AS heap_only;

-- ── Kiểu 1: OLTP point lookup ────────────────────────────────────────────────────
-- Chưa có index → Seq Scan: đọc toàn bộ heap (mọi page), lọc từng dòng, trả 1 dòng.
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM pos_transactions WHERE transaction_id = 'HN01-20260215-00010';

CREATE INDEX idx_pos_transactions_txn ON pos_transactions (transaction_id);

-- Có index → Index Scan (hoặc Bitmap Heap Scan): đi B-tree tới đúng page, Buffers giảm còn vài page.
-- Đây là "OLTP": nhiều câu hỏi nhỏ, mỗi câu đụng vài dòng, index là tất cả.
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM pos_transactions WHERE transaction_id = 'HN01-20260215-00010';

-- ── Kiểu 2: OLAP aggregate ───────────────────────────────────────────────────────
-- Plan: Sort ← HashAggregate ← Seq Scan (Filter: store_id IS NOT NULL).
-- Index idx_pos_transactions_store KHÔNG được dùng: câu hỏi cần ~100% dòng, đi qua index rồi quay lại
-- heap từng dòng còn đắt hơn đọc thẳng heap. Planner biết điều đó nhờ thống kê (ANALYZE đã chạy khi seed).
EXPLAIN (ANALYZE, BUFFERS)
SELECT
    (ts AT TIME ZONE 'Asia/Ho_Chi_Minh')::date AS business_date,
    store_id,
    sum(total_amount_vnd)                      AS revenue_vnd
FROM pos_transactions
WHERE store_id IS NOT NULL
GROUP BY 1, 2
ORDER BY 1, 2;

-- TODO 3 — trả lời: với heap, câu hỏi 3/50 cột vẫn đọc 100% page (mỗi page chứa đủ 50 cột của ~N dòng).
-- Column store chỉ đọc 3/50 cột ≈ 6% I/O, chưa kể nén theo cột (cùng kiểu dữ liệu nén tốt hơn).
-- Đó là lý do warehouse/lake dùng Parquet/ORC/columnar engine, còn POS vẫn dùng Postgres: hai việc khác nhau.

-- Kết luận thực tế cho Phin Phố: KHÔNG chạy dashboard trực tiếp trên Postgres của POS.
-- Mỗi lần dashboard refresh là một Seq Scan cạnh tranh I/O với máy tính tiền đang bán hàng.
-- Copy sang lake (video #07) rồi hỏi ở đó.

DROP INDEX IF EXISTS idx_pos_transactions_txn;
