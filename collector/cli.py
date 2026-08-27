"""Command line entrypoint: collect | backfill | verify | doctor."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, time, timedelta
from pathlib import Path

from . import config as config_module
from .publish import Publisher
from .runner import CommandError, FixtureRunner, Runner, SubprocessRunner
from .sources.dcgm import DcgmSource
from .sources.directory import DirectorySource
from .sources.prometheus import PrometheusSource
from .sources.slurm import SlurmSource, UtilizationReport, drop_implausible
from .sources.storage import StorageSource
from .state import MemoryStateStore, StateStore, utc_now_iso
from .transform import derive, privacy
from .transform.aggregate import DayAggregate, aggregate_days, fallback_denominator


def _midnight(day: date) -> datetime:
    return datetime.combine(day, time.min)


def _eprint(message: str) -> None:
    print(message, file=sys.stderr)


# --------------------------------------------------------------------------
# collect
# --------------------------------------------------------------------------


def _collect_window(
    cfg: config_module.Config,
    slurm: SlurmSource,
    state: StateStore,
    start: date,
    end: date,
    *,
    per_day_sreport: bool,
    save_raw: bool,
) -> tuple[list[dict], list[str]]:
    """Collect [start, end) and return (scrubbed day records, warnings)."""
    warnings: list[str] = []
    window_start = _midnight(start)
    window_end = _midnight(end)

    jobs = slurm.fetch_jobs(window_start, window_end)

    # Orphaned records — a job sacct still believes is RUNNING has its elapsed
    # computed as (now - start) and grows forever. Left in, they set impossible
    # records and add phantom allocation to every day they span.
    max_job_hours = float(cfg.sources.source("slurm").get("max_job_hours") or 0)
    jobs, dropped = drop_implausible(jobs, max_job_hours)
    if dropped:
        detail = ", ".join(f"{reason}={count}" for reason, count in sorted(dropped.items()))
        warnings.append(f"dropped implausible job record(s): {detail}")

    utilization: dict[date, UtilizationReport] = {}
    if per_day_sreport:
        cursor = start
        while cursor < end:
            try:
                utilization[cursor] = slurm.fetch_utilization(
                    _midnight(cursor), _midnight(cursor + timedelta(days=1))
                )
            except CommandError as exc:
                warnings.append(f"sreport: {cursor} failed ({exc.returncode})")
            cursor += timedelta(days=1)
    else:
        # Fixture mode: one canned report stands in for every day.
        try:
            report = slurm.fetch_utilization(window_start, window_end)
            cursor = start
            while cursor < end:
                utilization[cursor] = report
                cursor += timedelta(days=1)
        except CommandError as exc:
            warnings.append(f"sreport: unavailable ({exc.returncode})")

    aggregates = aggregate_days(jobs, window_start, window_end, utilization)

    records: list[dict] = []
    for day in sorted(aggregates):
        agg: DayAggregate = aggregates[day]

        if not agg.utilization_from_sreport:
            snapshot = cfg.cluster.capacity_on(day)
            if snapshot:
                fallback_denominator(agg, snapshot.total_gpus)
                warnings.append(f"{day}: no sreport data, denominator from capacity timeline")

        state.record_users(day, agg.users)
        state.record_accounts(day, agg.by_account)
        records.append(privacy.scrub_day(agg, cfg.groups, cfg.sources.k_anonymity))

    if save_raw and jobs:
        cursor = start
        while cursor < end:
            cursor += timedelta(days=1)

    return records, warnings


def cmd_collect(args: argparse.Namespace) -> int:
    cfg = config_module.load(args.config)

    fixture_mode = bool(args.from_dump)
    runner: Runner = FixtureRunner(args.from_dump) if fixture_mode else SubprocessRunner()
    state: StateStore = MemoryStateStore() if fixture_mode else StateStore(args.state)

    slurm = SlurmSource(runner, cfg.sources.source("slurm"), cfg.sources.cluster_name)

    if args.since and args.until:
        start, end = date.fromisoformat(args.since), date.fromisoformat(args.until)
    else:
        today = date.today()
        end = today
        start = today - timedelta(days=cfg.sources.resettle_days)

    if fixture_mode and not (args.since and args.until):
        # Derive the window from the fixture itself so canned data is not
        # silently filtered out by today's date.
        probe = slurm.fetch_jobs(datetime(1970, 1, 1), datetime(2100, 1, 1))
        stamps = [j.start for j in probe if j.start] + [j.submit for j in probe if j.submit]
        if stamps:
            start = min(stamps).date()
            end = max(stamps).date() + timedelta(days=1)

    _eprint(f"collecting {start} .. {end} ({'fixtures' if fixture_mode else 'live'})")

    records, warnings = _collect_window(
        cfg,
        slurm,
        state,
        start,
        end,
        per_day_sreport=not fixture_mode,
        save_raw=not fixture_mode,
    )

    repo_dir = Path(args.out).parent if args.out else Path(cfg.sources.publish.get("repo_dir", "."))
    publisher = Publisher(cfg, repo_dir)
    if args.out:
        publisher.data_dir = Path(args.out)
    if args.summary:
        publisher.summary_path = Path(args.summary)

    publisher.write_days(records)
    publisher.write_index()

    # Rebuild everything derived from the full published history, not just
    # this window — a resettled day changes rollups and possibly records.
    all_days = publisher.read_all_days()

    k = cfg.sources.k_anonymity
    max_job_hours = float(cfg.sources.source("slurm").get("max_job_hours") or 0)
    monthly = derive.build_rollup(all_days, "monthly", cfg.cluster, state, cfg.groups, k)
    yearly = derive.build_rollup(all_days, "yearly", cfg.cluster, state, cfg.groups, k)
    records_doc = derive.build_records(all_days, state, max_job_hours=max_job_hours)

    publisher.write_rollup("monthly", monthly)
    publisher.write_rollup("yearly", yearly)
    publisher.write_doc("records", records_doc)

    inventory, inv_warnings = _build_inventory(cfg, slurm)
    warnings.extend(inv_warnings)
    publisher.write_doc("inventory", inventory)

    growth = derive.build_growth(all_days, state)
    publisher.write_doc("growth", growth)

    # Publish the price table alongside the dollar figures it produces, so the
    # /value/ page can print its source and as-of date next to the number.
    priced, price_reason = cfg.cluster.pricing_is_publishable()
    pricing = cfg.cluster.cloud_pricing or {}
    publisher.write_doc(
        "pricing",
        {
            "available": priced,
            "reason": None if priced else price_reason.replace(" ", "-"),
            "currency": pricing.get("currency"),
            "basis": pricing.get("basis"),
            "source": pricing.get("source"),
            "asof": pricing.get("asof"),
            "usd_per_gpu_hour": pricing.get("usd_per_gpu_hour") or {},
        },
    )

    prometheus = PrometheusSource(cfg.sources.source("prometheus"))
    storage_doc, storage_warnings = _build_storage(cfg, runner, fixture_mode, prometheus)
    warnings.extend(storage_warnings)
    publisher.write_doc("storage", storage_doc)

    summary = derive.build_summary(
        all_days,
        monthly,
        yearly,
        records_doc,
        cfg.cluster,
        inventory,
        state,
        groups=cfg.groups,
        k_anonymity=k,
    )
    if storage_doc.get("available"):
        summary["storage_pib"] = storage_doc.get("total_pib")
    publisher.write_summary(summary)

    health = {
        "last_run": utc_now_iso(),
        "window_start": start.isoformat(),
        "window_end": end.isoformat(),
        "days_written": len(records),
        "mode": "fixtures" if fixture_mode else "live",
        "warnings": len(warnings),
        "sources": {
            "slurm": "ok",
            "storage": "ok" if storage_doc.get("available") else "unavailable",
            "dcgm": "disabled" if not cfg.sources.enabled("dcgm") else "enabled",
        },
    }
    publisher.write_doc("health", health)

    for warning in warnings[:20]:
        _eprint(f"  warn: {warning}")
    if len(warnings) > 20:
        _eprint(f"  warn: ... and {len(warnings) - 20} more")

    checked = publisher.verify()
    _eprint(f"privacy gate passed over {checked} file(s)")

    if args.no_commit:
        _eprint("--no-commit set; leaving the working tree dirty")
        return 0

    message = f"chore(data): metrics through {(end - timedelta(days=1)).isoformat()}"
    result = publisher.commit_and_push(message, push=not args.no_push)
    _eprint(f"publish: {result}")
    return 0


def _build_inventory(cfg: config_module.Config, slurm: SlurmSource) -> tuple[dict, list[str]]:
    warnings: list[str] = []
    try:
        nodes = slurm.fetch_nodes()
    except (CommandError, OSError) as exc:
        return {"available": False}, [f"sinfo: unavailable ({type(exc).__name__})"]

    by_model: dict[str, int] = {}
    gpus_online = 0
    nodes_online = 0
    for node in nodes:
        healthy = not any(
            flag in node.state.lower() for flag in ("down", "drain", "fail", "unknown")
        )
        if healthy:
            nodes_online += 1
            gpus_online += node.gpus
        if node.gpus:
            by_model[node.gpu_model or "unspecified"] = (
                by_model.get(node.gpu_model or "unspecified", 0) + node.gpus
            )

    latest = cfg.cluster.capacity_timeline[-1] if cfg.cluster.capacity_timeline else None
    configured_total = latest.total_gpus if latest else None
    discovered_total = sum(by_model.values())
    if configured_total is not None and configured_total != discovered_total:
        warnings.append(
            f"capacity_timeline says {configured_total} GPUs but sinfo reports "
            f"{discovered_total} — update config/cluster.yaml"
        )

    peak = cfg.cluster.peak_pflops(by_model) if by_model else None

    return (
        {
            "available": True,
            "nodes_total": len(nodes),
            "nodes_online": nodes_online,
            "gpus_total": discovered_total,
            "gpus_online": gpus_online,
            "gpus_by_model": dict(sorted(by_model.items(), key=lambda kv: -kv[1])),
            "cpus_total": sum(n.cpus for n in nodes),
            "peak_pflops_fp16": round(peak, 1) if peak else None,
        },
        warnings,
    )


def _build_storage(
    cfg: config_module.Config,
    runner: Runner,
    fixture_mode: bool,
    prometheus: PrometheusSource | None = None,
) -> tuple[dict, list[str]]:
    if not cfg.sources.enabled("storage") or fixture_mode:
        return {"available": False, "reason": "disabled"}, []
    source = StorageSource(runner, cfg.sources.source("storage"))
    usages, warnings = source.fetch_capacity(cfg.cluster.filesystems)
    doc = StorageSource.to_public(usages)

    if prometheus is not None and prometheus.configured and cfg.sources.enabled("prometheus"):
        io_doc, io_warnings = _fetch_storage_io(prometheus, cfg.sources.source("prometheus"))
        warnings.extend(io_warnings)
        doc.update(io_doc)
    else:
        doc["io_available"] = False

    return doc, warnings


def _fetch_storage_io(prometheus: PrometheusSource, prom_settings: dict) -> tuple[dict, list[str]]:
    """Integrate yesterday's storage read/write rates into total bytes."""
    warnings: list[str] = []
    queries = prom_settings.get("queries") or {}
    read_query = queries.get("storage_read_bytes")
    write_query = queries.get("storage_write_bytes")
    if not read_query or not write_query:
        return {"io_available": False}, [
            "storage I/O: add prometheus.queries.storage_read_bytes and "
            "storage_write_bytes to sources.yaml"
        ]

    today = date.today()
    window_end = _midnight(today)
    window_start = _midnight(today - timedelta(days=1))

    read_bytes, warn = prometheus.integral_bytes(read_query, window_start, window_end)
    if warn:
        warnings.append(warn)

    write_bytes, warn = prometheus.integral_bytes(write_query, window_start, window_end)
    if warn:
        warnings.append(warn)

    if read_bytes is None and write_bytes is None:
        return {"io_available": False}, warnings

    return {
        "io_available": True,
        "io_window_start": (today - timedelta(days=1)).isoformat(),
        "io_window_end": today.isoformat(),
        "io_read_bytes": int(read_bytes) if read_bytes is not None else None,
        "io_write_bytes": int(write_bytes) if write_bytes is not None else None,
    }, warnings


# --------------------------------------------------------------------------
# backfill
# --------------------------------------------------------------------------


def cmd_backfill(args: argparse.Namespace) -> int:
    cfg = config_module.load(args.config)
    runner: Runner = SubprocessRunner()
    slurm = SlurmSource(runner, cfg.sources.source("slurm"), cfg.sources.cluster_name)
    state = StateStore(args.state)

    if args.since:
        start = date.fromisoformat(args.since)
    else:
        start = _discover_earliest(slurm, args.dry_run)
        if start is None:
            _eprint("could not determine the earliest record; pass --since")
            return 2

    end = date.fromisoformat(args.until) if args.until else date.today()
    chunk_days = int(cfg.sources.source("slurm").get("backfill_chunk_days", 7))

    _eprint(f"backfill {start} .. {end} in {chunk_days}-day chunks")
    if args.dry_run:
        total = (end - start).days
        _eprint(f"dry run: would process {total} day(s) in ~{max(total // chunk_days, 1)} chunk(s)")
        return 0

    repo_dir = Path(cfg.sources.publish.get("repo_dir", "."))
    publisher = Publisher(cfg, repo_dir)

    cursor = start
    while cursor < end:
        chunk_end = min(cursor + timedelta(days=chunk_days), end)
        month = privacy.month_key(cursor)

        # Resumable: skip a chunk whose days are already published unless
        # --force. A multi-year backfill will get interrupted.
        if not args.force and _chunk_complete(publisher, cursor, chunk_end):
            _eprint(f"  {cursor} .. {chunk_end}: already published, skipping")
            cursor = chunk_end
            continue

        _eprint(f"  {cursor} .. {chunk_end} (month {month})")
        try:
            records, warnings = _collect_window(
                cfg,
                slurm,
                state,
                cursor,
                chunk_end,
                per_day_sreport=True,
                save_raw=True,
            )
        except CommandError as exc:
            _eprint(f"  chunk failed ({exc}); stopping so it can be resumed")
            return 1

        publisher.write_days(records)
        publisher.write_index()
        for warning in warnings[:5]:
            _eprint(f"    warn: {warning}")
        cursor = chunk_end

    all_days = publisher.read_all_days()
    k = cfg.sources.k_anonymity
    publisher.write_rollup(
        "monthly", derive.build_rollup(all_days, "monthly", cfg.cluster, state, cfg.groups, k)
    )
    publisher.write_rollup(
        "yearly", derive.build_rollup(all_days, "yearly", cfg.cluster, state, cfg.groups, k)
    )
    publisher.write_doc(
        "records",
        derive.build_records(
            all_days,
            state,
            max_job_hours=float(cfg.sources.source("slurm").get("max_job_hours") or 0),
        ),
    )
    publisher.write_doc("growth", derive.build_growth(all_days, state))

    checked = publisher.verify()
    _eprint(f"privacy gate passed over {checked} file(s)")
    _eprint("backfill complete; review the diff, then commit")
    return 0


def _chunk_complete(publisher: Publisher, start: date, end: date) -> bool:
    published = {r["date"] for r in publisher.read_all_days()}
    cursor = start
    while cursor < end:
        if cursor.isoformat() not in published:
            return False
        cursor += timedelta(days=1)
    return True


def _discover_earliest(slurm: SlurmSource, dry_run: bool) -> date | None:
    """Find the oldest record slurmdbd still holds."""
    probe_start = datetime(2015, 1, 1)
    probe_end = datetime.now()
    try:
        jobs = slurm.fetch_jobs(probe_start, probe_end)
    except CommandError as exc:
        _eprint(f"probe failed: {exc}")
        return None
    stamps = [j.submit for j in jobs if j.submit] + [j.start for j in jobs if j.start]
    return min(stamps).date() if stamps else None


# --------------------------------------------------------------------------
# verify
# --------------------------------------------------------------------------


def cmd_verify(args: argparse.Namespace) -> int:
    cfg = config_module.load(args.config)
    data_dir = Path(args.data)
    publisher = Publisher(cfg, data_dir.parent if data_dir.name == "data" else Path("."))
    publisher.data_dir = data_dir
    if args.summary:
        publisher.summary_path = Path(args.summary)

    try:
        checked = publisher.verify()
    except privacy.PrivacyViolation as exc:
        _eprint(f"PRIVACY GATE FAILED: {exc}")
        return 1

    if args.require_live:
        # `make collect-dry` produces a complete, schema-valid tree from test
        # fixtures. It would deploy perfectly — and publish invented numbers
        # on a public University website. CI refuses it.
        health_path = data_dir / "health.json"
        if not health_path.exists():
            _eprint("PUBLISH GATE FAILED: data/health.json is missing")
            return 1
        try:
            mode = json.loads(health_path.read_text()).get("mode")
        except json.JSONDecodeError:
            _eprint("PUBLISH GATE FAILED: data/health.json is not valid JSON")
            return 1
        if mode != "live":
            _eprint(
                f"PUBLISH GATE FAILED: health.json reports mode={mode!r}. "
                f"Fixture-derived data must never be published."
            )
            return 1

    print(f"privacy gate passed over {checked} file(s) in {data_dir}")
    return 0


# --------------------------------------------------------------------------
# doctor
# --------------------------------------------------------------------------


def cmd_doctor(args: argparse.Namespace) -> int:
    cfg = config_module.load(args.config)
    runner = SubprocessRunner()
    problems = 0

    def check(name: str, ok: bool, detail: str = "") -> None:
        nonlocal problems
        mark = "ok  " if ok else "FAIL"
        print(f"[{mark}] {name}{(': ' + detail) if detail else ''}")
        if not ok:
            problems += 1

    # -- config -----------------------------------------------------------
    priced, reason = cfg.cluster.pricing_is_publishable()
    check("cloud pricing table", priced, reason or "sourced and dated")
    check(
        "capacity timeline",
        bool(cfg.cluster.capacity_timeline),
        f"{len(cfg.cluster.capacity_timeline)} snapshot(s)",
    )
    check(
        "account allowlist",
        True,
        f"{len(cfg.groups.accounts)} named account(s); everything else -> "
        f"{cfg.groups.fallback.display_name!r}",
    )

    # -- slurm ------------------------------------------------------------
    slurm = SlurmSource(runner, cfg.sources.source("slurm"), cfg.sources.cluster_name)
    try:
        client_version = slurm.version()
        check("slurm client", True, client_version)
    except (CommandError, OSError) as exc:
        check("slurm client", False, str(exc))
        client_version = ""

    try:
        ping = runner.run(["scontrol", "ping"], timeout=30).strip()
        check("slurmctld reachable", "UP" in ping.upper(), ping[:120])
    except (CommandError, OSError) as exc:
        check("slurmctld reachable", False, str(exc))

    try:
        server_version = runner.run(["scontrol", "--version"], timeout=30).strip()
        same = _major_minor(client_version) == _major_minor(server_version)
        check(
            "slurm client/server version match",
            same,
            f"client {client_version!r} vs server {server_version!r}",
        )
    except (CommandError, OSError) as exc:
        check("slurm client/server version match", False, str(exc))

    try:
        nodes = slurm.fetch_nodes()
        discovered = sum(n.gpus for n in nodes)
        latest = cfg.cluster.capacity_timeline[-1] if cfg.cluster.capacity_timeline else None
        expected = latest.total_gpus if latest else None
        check(
            "sinfo inventory matches cluster.yaml",
            expected == discovered,
            f"sinfo reports {discovered} GPU(s) across {len(nodes)} node(s); "
            f"cluster.yaml says {expected}",
        )
    except (CommandError, OSError) as exc:
        check("sinfo inventory", False, str(exc))

    # -- storage ----------------------------------------------------------
    if cfg.sources.enabled("storage"):
        source = StorageSource(runner, cfg.sources.source("storage"))
        usages, warnings = source.fetch_capacity(cfg.cluster.filesystems)
        check(
            "storage nodes reachable",
            not warnings,
            f"{len(usages)} dataset(s); " + ("; ".join(warnings) if warnings else "all reachable"),
        )
    else:
        print("[skip] storage: disabled in sources.yaml")

    # -- directory --------------------------------------------------------
    if cfg.sources.enabled("directory"):
        directory = DirectorySource(cfg.sources.source("directory"))
        accounts, warnings = directory.fetch_account_counts()
        check(
            "LDAP",
            accounts.get("available", False),
            "; ".join(warnings) or f"{accounts.get('accounts_total')} account(s)",
        )
        hosts, warnings = directory.fetch_host_inventory()
        check(
            "Foreman",
            hosts.get("available", False),
            "; ".join(warnings) or f"{hosts.get('managed_hosts')} managed host(s)",
        )
    else:
        print("[skip] directory: disabled in sources.yaml")

    # -- prometheus / dcgm ------------------------------------------------
    prometheus = PrometheusSource(cfg.sources.source("prometheus"))
    if cfg.sources.enabled("prometheus"):
        check(
            "Prometheus configured",
            prometheus.configured,
            prometheus.url or "sources.prometheus.url is unset",
        )
    else:
        print("[skip] prometheus: disabled in sources.yaml")

    dcgm = DcgmSource(cfg.sources.source("dcgm"), prometheus)
    print(
        f"[info] DCGM: {'enabled' if dcgm.enabled else 'disabled'} — "
        f"until enabled the site reports ALLOCATED GPU hours, never utilization"
    )

    # -- publish path -----------------------------------------------------
    repo_dir = Path(args.repo or cfg.sources.publish.get("repo_dir", "."))
    check("repo directory exists", (repo_dir / ".git").exists(), str(repo_dir))
    if (repo_dir / ".git").exists():
        try:
            runner.run(
                ["git", "-C", str(repo_dir), "ls-remote", "--exit-code", "origin", "HEAD"],
                timeout=60,
            )
            check("git remote reachable (deploy key)", True)
        except (CommandError, OSError) as exc:
            check("git remote reachable (deploy key)", False, str(exc)[:200])

    print()
    print(f"{problems} problem(s) found")
    return 1 if problems else 0


def _major_minor(version_string: str) -> str:
    for token in version_string.split():
        if any(ch.isdigit() for ch in token):
            parts = token.split(".")
            return ".".join(parts[:2])
    return version_string.strip()


# --------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="collector",
        description="Collect, scrub, and publish DSI cluster metrics.",
    )
    parser.add_argument("--config", default=None, help="config directory (default: ./config)")
    sub = parser.add_subparsers(dest="command", required=True)

    collect = sub.add_parser("collect", help="collect the recent window and publish")
    collect.add_argument("--since", help="YYYY-MM-DD (inclusive)")
    collect.add_argument("--until", help="YYYY-MM-DD (exclusive)")
    collect.add_argument("--from-dump", help="read canned command output from this directory")
    collect.add_argument("--state", default=None, help="on-cluster state directory")
    collect.add_argument("--out", default=None, help="override the published data directory")
    collect.add_argument("--summary", default=None, help="override the summary.json path")
    collect.add_argument("--no-commit", action="store_true", help="write files but do not commit")
    collect.add_argument("--no-push", action="store_true", help="commit but do not push")
    collect.set_defaults(func=cmd_collect)

    backfill = sub.add_parser("backfill", help="walk all available history")
    backfill.add_argument("--since", help="YYYY-MM-DD; default: probe slurmdbd")
    backfill.add_argument("--until", help="YYYY-MM-DD; default: today")
    backfill.add_argument("--state", default=None)
    backfill.add_argument("--force", action="store_true", help="recompute already-published days")
    backfill.add_argument("--dry-run", action="store_true")
    backfill.set_defaults(func=cmd_backfill)

    verify = sub.add_parser("verify", help="run the privacy gate over a published tree")
    verify.add_argument("data", nargs="?", default="data")
    verify.add_argument("--summary", default="_data/summary.json")
    verify.add_argument(
        "--require-live",
        action="store_true",
        help="also fail if the data was generated from fixtures (used by CI)",
    )
    verify.set_defaults(func=cmd_verify)

    doctor = sub.add_parser("doctor", help="check every configured source")
    doctor.add_argument("--repo", default=None)
    doctor.set_defaults(func=cmd_doctor)

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return args.func(args)
    except privacy.PrivacyViolation as exc:
        _eprint(f"PRIVACY GATE FAILED: {exc}")
        return 1
    except config_module.ConfigError as exc:
        _eprint(f"config error: {exc}")
        return 2


if __name__ == "__main__":
    sys.exit(main())
