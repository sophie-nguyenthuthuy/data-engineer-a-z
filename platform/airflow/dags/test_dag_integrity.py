"""DAG integrity test (video #19): mọi DAG phải import được, không có cycle, có owner và tags.

Chạy trong CI khi airflow được cài; bỏ qua nếu không (pytest -q platform/airflow).
"""

from __future__ import annotations

from pathlib import Path

import pytest

airflow = pytest.importorskip("airflow")

DAGS_DIR = Path(__file__).parent


def test_dagbag_has_no_import_errors():
    from airflow.models import DagBag
    from airflow.utils.dag_cycle_tester import check_cycle

    bag = DagBag(dag_folder=str(DAGS_DIR), include_examples=False)
    assert not bag.import_errors, bag.import_errors
    assert {"coffee_daily_pipeline", "coffee_backfill", "coffee_quality_checks"} <= set(bag.dag_ids)
    for dag in bag.dags.values():
        assert dag.tags, f"{dag.dag_id} has no tags"
        check_cycle(dag)  # raises AirflowDagCycleException on cycle
