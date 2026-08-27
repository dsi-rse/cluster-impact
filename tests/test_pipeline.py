"""End-to-end: fixtures in, published tree out, privacy gate green.

This is the test that `make collect-dry` mirrors — the whole pipeline runs on
a laptop with no Slurm, no LDAP, and no cluster network.
"""

import json
from pathlib import Path

from collector.cli import main

CONFIG_DIR = Path(__file__).parent / "config"
FIXTURES = Path(__file__).parent / "fixtures"


def _run(tmp_path: Path) -> tuple[int, Path, Path]:
    data_dir = tmp_path / "data"
    summary = tmp_path / "_data" / "summary.json"
    code = main(
        [
            "--config",
            str(CONFIG_DIR),
            "collect",
            "--from-dump",
            str(FIXTURES),
            "--out",
            str(data_dir),
            "--summary",
            str(summary),
            "--no-commit",
        ]
    )
    return code, data_dir, summary


def test_pipeline_runs_clean(tmp_path):
    code, data_dir, summary = _run(tmp_path)
    assert code == 0
    assert (data_dir / "daily" / "2026-07.json").exists()
    assert (data_dir / "rollups" / "monthly.json").exists()
    assert (data_dir / "rollups" / "yearly.json").exists()
    assert (data_dir / "records.json").exists()
    assert (data_dir / "inventory.json").exists()
    assert (data_dir / "health.json").exists()
    assert summary.exists()


def test_month_file_has_all_three_days(tmp_path):
    _, data_dir, _ = _run(tmp_path)
    payload = json.loads((data_dir / "daily" / "2026-07.json").read_text())
    assert payload["month"] == "2026-07"
    assert [d["date"] for d in payload["days"]] == [
        "2026-07-20",
        "2026-07-21",
        "2026-07-22",
    ]


def test_rerun_is_idempotent(tmp_path):
    _run(tmp_path)
    first = (tmp_path / "data" / "daily" / "2026-07.json").read_text()
    _run(tmp_path)
    second = json.loads((tmp_path / "data" / "daily" / "2026-07.json").read_text())
    firstj = json.loads(first)
    # `generated` is a timestamp and is expected to move; the data must not.
    assert firstj["days"] == second["days"]


def test_inventory_is_discovered_from_sinfo(tmp_path):
    _, data_dir, _ = _run(tmp_path)
    inventory = json.loads((data_dir / "inventory.json").read_text())
    assert inventory["available"] is True
    assert inventory["gpus_total"] == 56
    assert inventory["nodes_total"] == 10
    # gpu008 is draining, so it is counted as installed but not online.
    assert inventory["gpus_online"] == 48
    assert inventory["gpus_by_model"]["h100"] == 16


def test_summary_reports_both_denominators(tmp_path):
    _, _, summary_path = _run(tmp_path)
    summary = json.loads(summary_path.read_text())
    assert summary["utilization_ytd"] is not None
    assert summary["utilization_ytd_installed"] is not None
    # Available-capacity utilization must be the larger of the two.
    assert summary["utilization_ytd"] > summary["utilization_ytd_installed"]
    assert summary["availability_ytd"] is not None


def test_labs_are_counted_even_when_they_cannot_be_named(tmp_path):
    """Counting is not disclosure.

    solo-lab has one user, so k-anonymity forbids naming it — but it is still a
    lab that used the cluster, and the /community/ count says so. That is the
    whole point of separating classification from disclosure: the old behaviour
    tied the count to what could be named and undercounted real breadth of use.
    unlisted-lab is absent from the allowlist entirely, so it is neither
    classified nor counted.
    """
    _, data_dir, summary_path = _run(tmp_path)
    summary = json.loads(summary_path.read_text())

    # kolar-lab, dsi-clinic, cmsc-25025 AND solo-lab.
    assert summary["labs_courses_trailing_year"] == 4

    # Declared, not derived from job data.
    assert summary["departments_served"] == 3

    # And solo-lab is still not named anywhere in the published tree.
    blob = json.dumps([json.loads(p.read_text()) for p in sorted(data_dir.rglob("*.json"))])
    assert "Solo Lab" not in blob


def test_k_anonymity_is_applied_per_published_bucket(tmp_path):
    """A course with rotating students is anonymous daily but nameable monthly.

    CMSC 25025 has three distinct users across the fixture window but never
    more than two on any single day. It must therefore be suppressed in the
    daily series and named in the monthly rollup — suppressing it monthly
    would protect nobody while erasing real breadth of use.
    """
    _, data_dir, _ = _run(tmp_path)

    daily = json.loads((data_dir / "daily" / "2026-07.json").read_text())
    for day in daily["days"]:
        assert all(g["name"] != "CMSC 25025 Machine Learning" for g in day["groups"])

    monthly = json.loads((data_dir / "rollups" / "monthly.json").read_text())
    period = monthly["periods"][0]
    course = next((g for g in period["groups"] if g["name"] == "CMSC 25025 Machine Learning"), None)
    assert course is not None
    assert course["users"] == 3


def test_single_user_lab_stays_suppressed_at_every_granularity(tmp_path):
    _, data_dir, _ = _run(tmp_path)
    monthly = json.loads((data_dir / "rollups" / "monthly.json").read_text())
    yearly = json.loads((data_dir / "rollups" / "yearly.json").read_text())
    for rollup in (monthly, yearly):
        for period in rollup["periods"]:
            assert all(g["name"] != "Solo Lab" for g in period["groups"])


def test_records_document(tmp_path):
    _, data_dir, _ = _run(tmp_path)
    records = json.loads((data_dir / "records.json").read_text())
    assert records["available"] is True
    assert records["total_jobs"] == 30
    metrics = {e["metric"]: e for e in records["entries"]}
    assert metrics["largest_job_gpus"]["value"] == 16
    assert metrics["largest_job_gpus"]["date"] == "2026-07-21"


def test_verify_subcommand_passes_on_generated_tree(tmp_path):
    _, data_dir, summary = _run(tmp_path)
    code = main(["--config", str(CONFIG_DIR), "verify", str(data_dir), "--summary", str(summary)])
    assert code == 0


def test_verify_subcommand_fails_on_a_poisoned_tree(tmp_path, capsys):
    _, data_dir, summary = _run(tmp_path)
    path = data_dir / "daily" / "2026-07.json"
    payload = json.loads(path.read_text())
    payload["days"][0]["groups"].append(
        {
            "name": "Definitely Not Allowlisted Lab",
            "department": "Statistics",
            "division": "Physical Sciences Division",
            "type": "lab",
            "gpu_hours": 1.0,
            "cpu_hours": 1.0,
            "jobs": 1,
            "users": 9,
        }
    )
    path.write_text(json.dumps(payload))

    code = main(["--config", str(CONFIG_DIR), "verify", str(data_dir), "--summary", str(summary)])
    assert code == 1
    assert "PRIVACY GATE FAILED" in capsys.readouterr().err
