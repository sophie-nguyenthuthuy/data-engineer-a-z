"""Test lab #19 — kiểm tra cấu trúc DAG bằng `ast`, KHÔNG import airflow (không có trong .venv).

Chạy offline < 1 s. Test "thật" với DagBag nằm ở platform/airflow/dags/test_dag_integrity.py (chạy trong container).
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[3]
PLATFORM_DAGS = sorted(
    p
    for p in (REPO / "platform" / "airflow" / "dags").glob("*.py")
    if not p.name.startswith("test_")
)
ALL_DAG_FILES = [HERE / "coffee_weekly_report.py", *PLATFORM_DAGS]


def _dag_decorator_kwargs(tree: ast.Module) -> dict[str, ast.expr]:
    """Trả về kwargs của decorator @dag(...) đầu tiên tìm thấy trong file."""
    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef):
            continue
        for deco in node.decorator_list:
            if isinstance(deco, ast.Call) and getattr(deco.func, "id", None) == "dag":
                return {kw.arg: kw.value for kw in deco.keywords if kw.arg}
    raise AssertionError("no @dag(...) decorator found")


def _calls_datetime_now(tree: ast.Module) -> bool:
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr in {"now", "today", "utcnow"}
            and getattr(node.func.value, "id", None) in {"datetime", "date", "pendulum"}
        ):
            return True
    return False


@pytest.mark.parametrize("path", ALL_DAG_FILES, ids=lambda p: p.name)
def test_dag_has_id_schedule_and_tags(path: Path):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    kwargs = _dag_decorator_kwargs(tree)
    assert "dag_id" in kwargs, f"{path.name}: thiếu dag_id"
    assert isinstance(kwargs["dag_id"], ast.Constant) and kwargs["dag_id"].value
    assert "schedule" in kwargs, f"{path.name}: thiếu schedule (None cũng phải ghi rõ)"
    assert "tags" in kwargs and isinstance(kwargs["tags"], ast.List) and kwargs["tags"].elts
    assert "start_date" in kwargs
    assert isinstance(kwargs.get("catchup"), ast.Constant), "catchup phải được ghi rõ True/False"


@pytest.mark.parametrize("path", ALL_DAG_FILES, ids=lambda p: p.name)
def test_dag_does_not_use_now(path: Path):
    """datetime.now() trong DAG = backfill vô nghĩa. Ngày nghiệp vụ phải đến từ context ({{ ds }}, data_interval_*)."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    assert not _calls_datetime_now(tree), f"{path.name} dùng datetime.now()/today() — anti-pattern"


def test_weekly_report_schedule_is_monday_3am():
    tree = ast.parse((HERE / "coffee_weekly_report.py").read_text(encoding="utf-8"))
    kwargs = _dag_decorator_kwargs(tree)
    assert kwargs["dag_id"].value == "coffee_weekly_report"
    assert kwargs["schedule"].value == "0 3 * * 1"
    assert kwargs["catchup"].value is False


def test_weekly_sql_against_local_gold_if_present():
    """Nếu bạn đã `dbt build` (video #12) thì chạy luôn câu SQL của DAG trên DuckDB local."""
    import datetime as dt

    from query_top_stores import DEFAULT_DB, format_report, top_stores

    if not DEFAULT_DB.exists():
        pytest.skip("chưa có platform/dbt/target/coffee_gold.duckdb — chạy make smoke + dbt build")
    rows = top_stores(DEFAULT_DB, dt.date(2026, 2, 14), dt.date(2026, 2, 21))
    assert 1 <= len(rows) <= 5
    assert rows == sorted(rows, key=lambda r: r["revenue_vnd"], reverse=True)
    assert all(isinstance(r["revenue_vnd"], int) for r in rows)
    report = format_report(rows, dt.date(2026, 2, 14))
    assert report.startswith("Top 5 stores — week of 2026-02-14")
    assert "1. " in report
