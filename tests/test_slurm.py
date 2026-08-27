from datetime import date, datetime

from collector.sources.slurm import (
    JobRecord,
    SlurmSource,
    drop_implausible,
    implausible_reason,
    parse_alloc_tres,
    parse_gres,
    split_across_days,
)


def test_alloc_tres_plain_total():
    total, typed = parse_alloc_tres("billing=8,cpu=8,gres/gpu=2,mem=64G,node=1")
    assert total == 2
    assert typed == {}


def test_alloc_tres_typed_only():
    total, typed = parse_alloc_tres("cpu=16,gres/gpu:a100=4,mem=128G,node=1")
    assert total == 4
    assert typed == {"a100": 4}


def test_alloc_tres_does_not_double_count_when_both_present():
    # Slurm reports the plain total AND the typed breakdown. Summing both
    # would report 8 GPUs for a 4-GPU job.
    total, typed = parse_alloc_tres("cpu=16,gres/gpu=4,gres/gpu:h100=4,mem=128G")
    assert total == 4
    assert typed == {"h100": 4}


def test_alloc_tres_no_gpus():
    total, typed = parse_alloc_tres("billing=1,cpu=1,mem=8G,node=1")
    assert total == 0
    assert typed == {}


def test_split_across_days_crosses_midnight():
    chunks = dict(split_across_days(datetime(2026, 7, 20, 22, 0), datetime(2026, 7, 21, 6, 0)))
    assert chunks[date(2026, 7, 20)] == 2 * 3600
    assert chunks[date(2026, 7, 21)] == 6 * 3600


def test_split_across_days_spans_multiple_full_days():
    chunks = dict(split_across_days(datetime(2026, 7, 20, 12, 0), datetime(2026, 7, 23, 6, 0)))
    assert chunks[date(2026, 7, 20)] == 12 * 3600
    assert chunks[date(2026, 7, 21)] == 24 * 3600
    assert chunks[date(2026, 7, 22)] == 24 * 3600
    assert chunks[date(2026, 7, 23)] == 6 * 3600


def test_split_across_days_empty_for_inverted_interval():
    assert list(split_across_days(datetime(2026, 7, 21), datetime(2026, 7, 20))) == []


def test_job_record_strips_cancelled_by_uid():
    line = (
        "1014|nora|unlisted-lab|general|general|CANCELLED by 40021|"
        "2026-07-20T15:00:00|2026-07-20T15:30:00|2026-07-20T16:00:00|1800|"
        "billing=4,cpu=4,gres/gpu:l40s=1,mem=32G,node=1|1|4|0:0"
    )
    record = JobRecord.parse(line)
    assert record is not None
    # The uid in "CANCELLED by 40021" identifies a person; it must not survive.
    assert record.state == "CANCELLED"


def test_job_record_wait_seconds():
    record = JobRecord.parse(
        "1|u|a|p|q|COMPLETED|2026-07-20T08:00:00|2026-07-20T08:30:00|"
        "2026-07-20T09:00:00|1800|gres/gpu=1|1|1|0:0"
    )
    assert record.wait_seconds == 1800


def test_job_record_wait_none_when_never_started():
    record = JobRecord.parse(
        "1|u|a|p|q|CANCELLED|2026-07-20T08:00:00|Unknown|Unknown|0|gres/gpu=1|1|1|0:0"
    )
    assert record.wait_seconds is None
    assert not record.ran


def test_gpu_seconds_clipped_to_window():
    record = JobRecord.parse(
        "1|u|a|p|q|COMPLETED|2026-07-20T00:00:00|2026-07-20T00:00:00|"
        "2026-07-22T00:00:00|172800|gres/gpu=2|1|8|0:0"
    )
    by_day = record.gpu_seconds_by_day(datetime(2026, 7, 21), datetime(2026, 7, 22))
    assert set(by_day) == {date(2026, 7, 21)}
    assert by_day[date(2026, 7, 21)] == 2 * 86400


def test_parse_gres_variants():
    assert parse_gres("gpu:a100:4(S:0-1)") == (4, "a100")
    assert parse_gres("gpu:4") == (4, None)
    assert parse_gres("") == (0, None)
    assert parse_gres("(null)") == (0, None)


def test_utilization_available_excludes_downtime():
    report = SlurmSource._parse_utilization(
        "dsicluster|gres/gpu|1728000|345600|0|1382400|0|3456000\n"
    )
    assert report.reported == 3456000
    assert report.down == 345600
    assert report.available == 3456000 - 345600
    assert report.availability_rate == (3456000 - 345600) / 3456000


# A real orphaned record from this cluster: submitted 2024-03-28, still
# reported RUNNING with no End, ElapsedRaw 74387706 (861 days). sacct computes
# elapsed for such a record as (now - start), so it grows every day forever.
_ZOMBIE = (
    "2|lup|general_group|general|normal|RUNNING|2024-03-28T16:42:15|"
    "2024-03-28T16:42:15|Unknown|74387706|billing=35,cpu=1,gres/gpu=1,mem=128G,node=1|1|1|0:0"
)
_REAL = (
    "999|someone|general_group|general|general|COMPLETED|2026-08-25T01:00:00|"
    "2026-08-25T01:00:00|2026-08-25T09:00:00|28800|"
    "billing=8,cpu=8,gres/gpu=2,mem=64G,node=1|1|8|0:0"
)


def test_orphaned_running_record_is_identified():
    job = JobRecord.parse(_ZOMBIE)
    assert job.state == "RUNNING"
    assert job.end is None
    assert implausible_reason(job, 96) == "stale-running"


def test_a_real_eight_hour_job_is_plausible():
    job = JobRecord.parse(_REAL)
    assert job.elapsed_seconds == 28800
    assert implausible_reason(job, 96) is None


def test_overlong_finished_job_is_flagged_distinctly():
    # A COMPLETED record beyond the ceiling is a different problem: it means a
    # partition limit was raised and max_job_hours needs revisiting.
    job = JobRecord.parse(_REAL.replace("|28800|", "|900000|"))
    assert implausible_reason(job, 96) == "over-max-walltime"


def test_ceiling_of_zero_disables_the_check():
    assert implausible_reason(JobRecord.parse(_ZOMBIE), 0) is None


def test_drop_implausible_reports_what_it_removed():
    jobs = [JobRecord.parse(_ZOMBIE), JobRecord.parse(_REAL), JobRecord.parse(_ZOMBIE)]
    kept, dropped = drop_implausible(jobs, 96)
    assert len(kept) == 1
    assert kept[0].job_id == "999"
    assert dropped == {"stale-running": 2}
