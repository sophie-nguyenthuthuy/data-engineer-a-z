# platform/quality — Data Quality (video #20)

Ba lớp, từ rẻ đến đắt:

| Lớp | Ở đâu | Bắt lỗi gì |
|-----|-------|------------|
| **Data contract** | `contracts/*.yaml` + `contract_check.py` (stdlib + polars) | schema đổi, cột mất, kiểu sai, giá trị ngoài tập cho phép |
| **dbt test** | `platform/dbt` | unique/not_null/relationships ở gold |
| **Great Expectations** | `suites/`, `run_checkpoint.py` | null rate, phân phối, freshness, số dòng lệch bất thường |

```bash
# contract check trên lake local (sau make smoke)
uv run python platform/quality/contract_check.py --contract platform/quality/contracts/pos_transactions.yaml \
    --parquet "lake-local/silver/pos_transactions/dt=2026-02-15/*.parquet"

# GX checkpoint
uv run --with "great-expectations>=1.1,<2" python platform/quality/run_checkpoint.py --date 2026-02-15 --lake ./lake-local
```

Contract là **hợp đồng giữa team nguồn (POS) và team data**: đổi schema phải đổi contract trước, PR review cả hai bên.
