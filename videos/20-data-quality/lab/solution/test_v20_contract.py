"""Test lab #20 — chạy pipeline vào tmp_path rồi kiểm tra contract line items. Offline, < 3 s.

contract_check.py không phải package (nằm ở platform/quality), nên load bằng importlib theo đường dẫn file.
"""

from __future__ import annotations

import copy
import importlib.util
from pathlib import Path
from types import ModuleType

import pytest
import yaml

from coffee_ingestion.lake import Lake
from coffee_ingestion.pipeline import run

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[3]
SAMPLES = REPO / "data" / "samples"
CONTRACTS = REPO / "platform" / "quality" / "contracts"
DATE = "2026-02-15"


def _load_contract_check() -> ModuleType:
    path = REPO / "platform" / "quality" / "contract_check.py"
    spec = importlib.util.spec_from_file_location("contract_check", path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def lake(tmp_path_factory: pytest.TempPathFactory) -> Lake:
    root = tmp_path_factory.mktemp("lake")
    for date in (DATE, "2026-02-17"):
        run(str(SAMPLES), str(root), date)
    return Lake(str(root))


@pytest.fixture(scope="module")
def line_items_contract() -> dict:
    return yaml.safe_load((HERE / "contracts" / "pos_line_items.yaml").read_text(encoding="utf-8"))


def test_solution_contract_passes_on_silver_line_items(lake: Lake, line_items_contract: dict):
    cc = _load_contract_check()
    df = lake.read_parquet("silver", "pos_line_items", f"dt={DATE}")
    assert df.height == 1392
    errors = cc.check(df, line_items_contract, DATE)
    assert errors == []


def test_broken_allowed_values_fail(lake: Lake, line_items_contract: dict):
    """Bước 4 của lab: bỏ 'L' khỏi allowed → contract phải FAIL và chỉ đúng cột size."""
    cc = _load_contract_check()
    broken = copy.deepcopy(line_items_contract)
    size_col = next(c for c in broken["columns"] if c["name"] == "size")
    size_col["allowed"] = ["S", "M"]
    df = lake.read_parquet("silver", "pos_line_items", f"dt={DATE}")
    errors = cc.check(df, broken, DATE)
    assert errors == ["size: values not allowed ['L']"]


def test_tet_low_volume_exception(lake: Lake):
    """Bước 2: mùng 1 Tết 59 dòng — qua nhờ exceptions, fail khi bỏ exceptions."""
    cc = _load_contract_check()
    contract = yaml.safe_load((CONTRACTS / "pos_transactions.yaml").read_text(encoding="utf-8"))
    df = lake.read_parquet("silver", "pos_transactions", "dt=2026-02-17")
    assert df.height == 59
    contract["sla"]["min_rows_per_day"] = 100
    assert cc.check(df, contract, "2026-02-17") == []
    contract["exceptions"]["low_volume_dates"] = []
    assert cc.check(df, contract, "2026-02-17") == ["only 59 rows < min_rows_per_day 100"]


def test_starter_contract_lists_every_silver_column(lake: Lake, line_items_contract: dict):
    """Starter và lời giải phải khai báo đúng các cột có trong silver (học viên chỉ điền ràng buộc)."""
    starter = yaml.safe_load(
        (HERE.parent / "contracts" / "pos_line_items.yaml").read_text(encoding="utf-8")
    )
    silver_cols = lake.read_parquet("silver", "pos_line_items", f"dt={DATE}").columns
    assert [c["name"] for c in starter["columns"]] == silver_cols
    assert [c["name"] for c in line_items_contract["columns"]] == silver_cols
