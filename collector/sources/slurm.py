"""Slurm accounting.

Three commands, three jobs:

  sacct    per-job detail — the numerator, plus users, accounts, wait, outcome
  sreport  the allocated/idle/down/planned split — the honest denominator
  sinfo    live node and GRES inventory — so config can't drift from reality

Deliberate omission: `JobName` is never requested. Job names on a research
cluster routinely carry grant numbers, subject identifiers, and unpublished
paper titles. Data we never collect cannot leak.
"""

from __future__ import annotations

import re
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta

from ..runner import Runner

# Field order requested from sacct. Keep in sync with JobRecord.parse.
SACCT_FIELDS = [
    "JobIDRaw",
    "User",
    "Account",
    "Partition",
    "QOS",
    "State",
    "Submit",
    "Start",
    "End",
    "ElapsedRaw",
    "AllocTRES",
    "NNodes",
    "NCPUS",
    "ExitCode",
]

_TRES_GPU_TYPED = re.compile(r"^gres/gpu:(?P<model>[^=]+)$")
_UNSET = {"", "Unknown", "None", "N/A", "(null)"}


def _parse_dt(value: str) -> datetime | None:
    value = (value or "").strip()
    if value in _UNSET:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S")
    except ValueError:
        return None


def _parse_int(value: str, default: int = 0) -> int:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return default


def parse_alloc_tres(tres: str) -> tuple[int, dict[str, int]]:
    """Extract GPU counts from an AllocTRES string.

    Slurm may report a plain total (`gres/gpu=2`), typed counts
    (`gres/gpu:a100=2`), or both at once. When both are present the plain key
    is authoritative for the total and the typed keys give the breakdown —
    summing everything would double-count.

    Returns (total_gpus, {model: count}).
    """
    total: int | None = None
    typed: dict[str, int] = {}

    for part in (tres or "").split(","):
        part = part.strip()
        if not part or "=" not in part:
            continue
        key, _, raw = part.partition("=")
        key = key.strip()
        if key == "gres/gpu":
            total = _parse_int(raw)
            continue
        match = _TRES_GPU_TYPED.match(key)
        if match:
            model = match.group("model").strip().lower()
            typed[model] = typed.get(model, 0) + _parse_int(raw)

    if total is None:
        total = sum(typed.values())
    return total, typed


def split_across_days(start: datetime, end: datetime) -> Iterator[tuple[date, float]]:
    """Split an interval into (calendar day, seconds on that day).

    A 30-hour job is not 30 GPU-hours on its start date. Without this, daily
    series spike on long-job start days and read as noise.
    """
    if end <= start:
        return
    cursor = start
    while cursor < end:
        day = cursor.date()
        midnight = datetime.combine(day + timedelta(days=1), datetime.min.time())
        chunk_end = min(midnight, end)
        yield day, (chunk_end - cursor).total_seconds()
        cursor = chunk_end


@dataclass
class JobRecord:
    job_id: str
    user: str
    account: str
    partition: str
    qos: str
    state: str
    submit: datetime | None
    start: datetime | None
    end: datetime | None
    elapsed_seconds: int
    gpus: int
    gpu_models: dict[str, int] = field(default_factory=dict)
    nodes: int = 1
    cpus: int = 1
    exit_code: str = ""

    @classmethod
    def parse(cls, line: str) -> JobRecord | None:
        parts = line.rstrip("\n").split("|")
        if len(parts) < len(SACCT_FIELDS):
            return None
        (
            job_id,
            user,
            account,
            partition,
            qos,
            state,
            submit,
            start,
            end,
            elapsed,
            alloc_tres,
            nnodes,
            ncpus,
            exit_code,
        ) = parts[: len(SACCT_FIELDS)]

        gpus, gpu_models = parse_alloc_tres(alloc_tres)

        return cls(
            job_id=job_id.strip(),
            user=user.strip(),
            account=account.strip().lower(),
            partition=partition.strip(),
            qos=qos.strip(),
            # "CANCELLED by 12345" -> "CANCELLED"; the uid is another person.
            state=state.strip().split()[0] if state.strip() else "UNKNOWN",
            submit=_parse_dt(submit),
            start=_parse_dt(start),
            end=_parse_dt(end),
            elapsed_seconds=_parse_int(elapsed),
            gpus=gpus,
            gpu_models=gpu_models,
            nodes=max(_parse_int(nnodes, 1), 1),
            cpus=max(_parse_int(ncpus, 1), 1),
            exit_code=exit_code.strip(),
        )

    @property
    def wait_seconds(self) -> float | None:
        """Queue wait. None when the job never started or has no submit time."""
        if self.submit is None or self.start is None:
            return None
        delta = (self.start - self.submit).total_seconds()
        return delta if delta >= 0 else None

    @property
    def ran(self) -> bool:
        return self.start is not None and self.elapsed_seconds > 0

    def gpu_seconds_by_day(self, window_start: datetime, window_end: datetime) -> dict[date, float]:
        """GPU-seconds attributed to each calendar day, clipped to the window."""
        return self._resource_seconds_by_day(self.gpus, window_start, window_end)

    def cpu_seconds_by_day(self, window_start: datetime, window_end: datetime) -> dict[date, float]:
        return self._resource_seconds_by_day(self.cpus, window_start, window_end)

    def _resource_seconds_by_day(
        self, units: int, window_start: datetime, window_end: datetime
    ) -> dict[date, float]:
        if units <= 0 or self.start is None:
            return {}
        # A job still running at snapshot time has no End; clip it to the
        # window edge and let the resettle window correct it on a later run.
        end = self.end or window_end
        lo = max(self.start, window_start)
        hi = min(end, window_end)
        out: dict[date, float] = {}
        for day, seconds in split_across_days(lo, hi):
            out[day] = out.get(day, 0.0) + seconds * units
        return out


def implausible_reason(job: JobRecord, max_hours: float) -> str | None:
    """Why this record cannot describe a real job, or None if it looks real.

    sacct computes ElapsedRaw for a job it still believes is RUNNING as
    (now - start). A job that was running when the controller lost track of it
    never receives an End time, so its elapsed grows without bound — three such
    records from 2024-03-28 were setting this site's "longest job" record at 861
    days. Those are accounting artifacts, not jobs, and they distort both the
    records wall and the allocation totals for every day they span.

    Returning a reason rather than a bool so the caller can report WHICH kind of
    bad record it dropped; a sudden run of `over-max-walltime` means a partition
    limit was raised and `max_job_hours` needs revisiting, which is a very
    different problem from an orphaned record.
    """
    if max_hours <= 0:
        return None
    if job.elapsed_seconds <= max_hours * 3600:
        return None
    if job.end is None and job.state == "RUNNING":
        return "stale-running"
    return "over-max-walltime"


def drop_implausible(
    jobs: list[JobRecord], max_hours: float
) -> tuple[list[JobRecord], dict[str, int]]:
    """Split job records into (usable, {reason: count})."""
    kept: list[JobRecord] = []
    dropped: dict[str, int] = {}
    for job in jobs:
        reason = implausible_reason(job, max_hours)
        if reason is None:
            kept.append(job)
        else:
            dropped[reason] = dropped.get(reason, 0) + 1
    return kept, dropped


def _field(parts: list[str], index: int) -> float:
    """One numeric column from a parsable2 row, tolerant of thousands separators."""
    try:
        return float(parts[index].replace(",", ""))
    except (ValueError, IndexError):
        return 0.0


@dataclass
class UtilizationReport:
    """sreport's cluster-utilization split, in TRES-seconds."""

    allocated: float = 0.0
    down: float = 0.0
    planned_down: float = 0.0
    idle: float = 0.0
    planned: float = 0.0
    reported: float = 0.0

    @property
    def available(self) -> float:
        """Capacity the scheduler could actually hand out.

        Everything reported, minus time the hardware was down or in a planned
        outage. This is the headline denominator.
        """
        return max(self.reported - self.down - self.planned_down, 0.0)

    @property
    def availability_rate(self) -> float | None:
        if self.reported <= 0:
            return None
        return self.available / self.reported


@dataclass
class NodeInfo:
    name: str
    state: str
    cpus: int
    memory_mb: int
    gpus: int
    gpu_model: str | None


_GRES_GPU = re.compile(r"gpu:(?:(?P<model>[a-zA-Z0-9_-]+):)?(?P<count>\d+)")


def parse_gres(gres: str) -> tuple[int, str | None]:
    """Parse a sinfo GRES string such as `gpu:a100:4(S:0-1)` or `gpu:4`."""
    text = (gres or "").strip()
    if not text or text in _UNSET:
        return 0, None
    total = 0
    model: str | None = None
    for match in _GRES_GPU.finditer(text):
        total += int(match.group("count"))
        if match.group("model") and model is None:
            model = match.group("model").lower()
    return total, model


class SlurmSource:
    def __init__(self, runner: Runner, settings: dict, cluster_name: str = "dsicluster"):
        self.runner = runner
        self.settings = settings or {}
        self.cluster_name = cluster_name

    # -- job detail -------------------------------------------------------

    def fetch_jobs(self, start: datetime, end: datetime) -> list[JobRecord]:
        argv = [
            self.settings.get("sacct_bin", "sacct"),
            "--allusers",
            "--allocations",  # allocations only, not job steps
            "--parsable2",
            "--noheader",
            "--start",
            start.strftime("%Y-%m-%dT%H:%M:%S"),
            "--end",
            end.strftime("%Y-%m-%dT%H:%M:%S"),
            "--format",
            ",".join(SACCT_FIELDS),
        ]
        out = self.runner.run(argv, timeout=1800)
        records = []
        for line in out.splitlines():
            if not line.strip():
                continue
            record = JobRecord.parse(line)
            if record is not None:
                records.append(record)
        return records

    # -- the denominator --------------------------------------------------

    def fetch_utilization(self, start: datetime, end: datetime) -> UtilizationReport:
        """GPU-seconds allocated / idle / down / planned, straight from sreport.

        Using sreport rather than deriving the denominator ourselves means the
        published utilization figure agrees with what an operator sees when
        they run the same command — which is exactly the cross-check a
        skeptical reader will perform.
        """
        argv = [
            self.settings.get("sreport_bin", "sreport"),
            "--parsable2",
            "--noheader",
            "-t",
            "Seconds",
            "-T",
            "gres/gpu",
            "cluster",
            "utilization",
            f"start={start.strftime('%Y-%m-%dT%H:%M:%S')}",
            f"end={end.strftime('%Y-%m-%dT%H:%M:%S')}",
        ]
        out = self.runner.run(argv, timeout=600)
        return self._parse_utilization(out)

    @staticmethod
    def _parse_utilization(out: str) -> UtilizationReport:
        # Cluster|TRES Name|Allocated|Down|PLND Down|Idle|Planned|Reported
        for line in out.splitlines():
            parts = [p.strip() for p in line.split("|")]
            if len(parts) < 8 or not parts[1].startswith("gres/gpu"):
                continue
            return UtilizationReport(
                allocated=_field(parts, 2),
                down=_field(parts, 3),
                planned_down=_field(parts, 4),
                idle=_field(parts, 5),
                planned=_field(parts, 6),
                reported=_field(parts, 7),
            )
        return UtilizationReport()

    # -- live inventory ---------------------------------------------------

    def fetch_nodes(self) -> list[NodeInfo]:
        argv = [
            self.settings.get("sinfo_bin", "sinfo"),
            "--noheader",
            "--Node",
            "--format",
            "%N|%G|%T|%c|%m",
        ]
        out = self.runner.run(argv, timeout=120)
        seen: dict[str, NodeInfo] = {}
        for line in out.splitlines():
            parts = [p.strip() for p in line.split("|")]
            if len(parts) < 5:
                continue
            name = parts[0]
            if name in seen:
                # A node in several partitions appears once per partition.
                continue
            gpus, model = parse_gres(parts[1])
            seen[name] = NodeInfo(
                name=name,
                state=parts[2],
                cpus=_parse_int(parts[3]),
                memory_mb=_parse_int(parts[4]),
                gpus=gpus,
                gpu_model=model,
            )
        return list(seen.values())

    def version(self) -> str:
        out = self.runner.run([self.settings.get("sacct_bin", "sacct"), "--version"], timeout=30)
        return out.strip()
