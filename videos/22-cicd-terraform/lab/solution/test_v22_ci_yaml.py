"""Test lab #22 — parse ci-extra.yml bằng PyYAML và kiểm tra cấu trúc workflow. Offline, < 1 s.

Gotcha nổi tiếng: YAML 1.1 đọc key `on` trần thành boolean True, nên test chấp nhận cả "on" và True.
"""

from __future__ import annotations

from pathlib import Path

import yaml

HERE = Path(__file__).resolve().parent
SOLUTION = HERE / "ci-extra.yml"
STARTER = HERE.parent / "ci-extra.yml"


def _load(path: Path) -> dict:
    doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(doc, dict)
    return doc


def _triggers(doc: dict) -> dict:
    on = doc.get("on", doc.get(True))
    assert on is not None, "thiếu khoá on:"
    return on


def _run_steps(job: dict) -> list[str]:
    return [s["run"] for s in job["steps"] if "run" in s]


def test_triggers_are_scoped_to_terraform_dir():
    on = _triggers(_load(SOLUTION))
    assert set(on) == {"pull_request", "push"}
    assert on["push"]["branches"] == ["main"]
    for event in ("pull_request", "push"):
        assert on[event]["paths"] == ["infra/terraform/**"]


def test_job_runs_fmt_init_without_backend_and_validate():
    doc = _load(SOLUTION)
    jobs = doc["jobs"]
    assert len(jobs) == 1
    job = next(iter(jobs.values()))
    assert job["runs-on"] == "ubuntu-latest"
    assert job["defaults"]["run"]["working-directory"] == "infra/terraform"

    uses = [s["uses"] for s in job["steps"] if "uses" in s]
    assert uses[0].startswith("actions/checkout@")
    setup = next(
        s for s in job["steps"] if s.get("uses", "").startswith("hashicorp/setup-terraform@")
    )
    assert setup["with"]["terraform_version"], "terraform_version phải được pin"

    runs = _run_steps(job)
    assert any(r.startswith("terraform fmt") and "-check" in r for r in runs)
    init = next(r for r in runs if r.startswith("terraform init"))
    assert "-backend=false" in init
    assert runs[-1].strip() == "terraform validate"
    # thứ tự: fmt → init → validate
    order = [next(k for k in ("fmt", "init", "validate") if k in r) for r in runs]
    assert order == ["fmt", "init", "validate"]


def test_starter_is_valid_yaml_but_incomplete():
    doc = _load(STARTER)
    assert _triggers(doc) == {}  # học viên phải điền
    job = next(iter(doc["jobs"].values()))
    assert _run_steps(job) == []  # chưa có step run nào
