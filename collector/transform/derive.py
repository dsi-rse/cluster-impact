"""Derived metrics: rollups, all-time records, headline summary.

Everything here consumes already-scrubbed day records, so it cannot
reintroduce identity. The one exception is unique-user counting, which reads
hashed user sets from the on-cluster state store and emits only integers.
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import date, timedelta
from typing import Any

from ..config import ClusterConfig, GroupsConfig
from ..state import StateStore

SECONDS_PER_HOUR = 3600.0
HOURS_PER_YEAR = 8760.0


def _sum(records: Iterable[dict], *path: str) -> float:
    total = 0.0
    for record in records:
        node: Any = record
        for key in path:
            node = (node or {}).get(key) if isinstance(node, dict) else None
        if isinstance(node, (int, float)) and not isinstance(node, bool):
            total += node
    return total


def _merge_maps(records: Iterable[dict], key: str) -> dict[str, float]:
    out: dict[str, float] = {}
    for record in records:
        for name, value in (record.get(key) or {}).items():
            out[name] = round(out.get(name, 0.0) + value, 2)
    return dict(sorted(out.items(), key=lambda kv: (-kv[1], kv[0])))


def _merge_counts(records: Iterable[dict], *path: str) -> dict[str, int]:
    out: dict[str, int] = {}
    for record in records:
        node: Any = record
        for key in path:
            node = (node or {}).get(key) if isinstance(node, dict) else None
        for name, value in (node or {}).items():
            out[name] = out.get(name, 0) + int(value)
    return dict(sorted(out.items()))


def _merge_groups(records: Iterable[dict]) -> list[dict[str, Any]]:
    """Combine per-day group entries into one entry per group.

    `users` is the MAX daily distinct count, not a sum — summing would both
    inflate the number and, worse, let a group whose daily count sits at the
    k-anonymity floor appear to clear it by accumulation.
    """
    merged: dict[str, dict[str, Any]] = {}
    for record in records:
        for entry in record.get("groups") or []:
            key = entry["name"]
            if key not in merged:
                merged[key] = {
                    "name": entry["name"],
                    "department": entry["department"],
                    "division": entry["division"],
                    "type": entry["type"],
                    "gpu_hours": 0.0,
                    "cpu_hours": 0.0,
                    "jobs": 0,
                    "users": 0,
                }
            merged[key]["gpu_hours"] += entry["gpu_hours"]
            merged[key]["cpu_hours"] += entry["cpu_hours"]
            merged[key]["jobs"] += entry["jobs"]
            merged[key]["users"] = max(merged[key]["users"], entry["users"])

    for entry in merged.values():
        entry["gpu_hours"] = round(entry["gpu_hours"], 2)
        entry["cpu_hours"] = round(entry["cpu_hours"], 2)

    return sorted(merged.values(), key=lambda e: (-e["gpu_hours"], e["name"]))


def groups_for_period(
    state: StateStore,
    groups: GroupsConfig,
    start: date,
    end: date,
    k_anonymity: int,
) -> list[dict[str, Any]] | None:
    """Build the group breakdown for a period from the on-cluster account index.

    k-anonymity is applied against the number of distinct users in THIS
    period, which is the bucket actually being published. Deriving rollups
    from already-suppressed daily records instead would permanently hide a
    course whose three students never overlap on a single day — the group is
    perfectly anonymous at monthly granularity, and suppressing it there
    protects nobody while erasing real breadth of use.

    Returns None when no account index exists (e.g. a repo-only rebuild), so
    the caller can fall back to merging published day records.
    """
    if not state.has_account_index():
        return None

    usage = state.accounts_between(start, end)
    if not usage:
        return None

    named: dict[str, dict[str, Any]] = {}
    other = {"gpu_seconds": 0.0, "cpu_seconds": 0.0, "jobs": 0, "users": set()}

    for account, entry in usage.items():
        identity = groups.resolve(account)
        users: set[str] = entry["users"] if isinstance(entry["users"], set) else set(entry["users"])
        if groups.is_named(account) and len(users) >= k_anonymity:
            key = identity.display_name
            bucket = named.setdefault(
                key,
                {
                    "name": identity.display_name,
                    "department": identity.department,
                    "division": identity.division,
                    "type": identity.type,
                    "gpu_seconds": 0.0,
                    "cpu_seconds": 0.0,
                    "jobs": 0,
                    "users": set(),
                },
            )
            bucket["gpu_seconds"] += entry["gpu_seconds"]
            bucket["cpu_seconds"] += entry["cpu_seconds"]
            bucket["jobs"] += entry["jobs"]
            bucket["users"] |= users
        else:
            other["gpu_seconds"] += entry["gpu_seconds"]
            other["cpu_seconds"] += entry["cpu_seconds"]
            other["jobs"] += entry["jobs"]
            other["users"] |= users

    out: list[dict[str, Any]] = []
    for bucket in named.values():
        # Merging two accounts under one display name can only grow the user
        # set, so the k check still holds after the merge.
        out.append(
            {
                "name": bucket["name"],
                "department": bucket["department"],
                "division": bucket["division"],
                "type": bucket["type"],
                "gpu_hours": round(bucket["gpu_seconds"] / SECONDS_PER_HOUR, 2),
                "cpu_hours": round(bucket["cpu_seconds"] / SECONDS_PER_HOUR, 2),
                "jobs": bucket["jobs"],
                "users": len(bucket["users"]),
            }
        )

    if other["jobs"] or other["gpu_seconds"] or other["cpu_seconds"]:
        out.append(
            {
                "name": groups.fallback.display_name,
                "department": groups.fallback.department,
                "division": groups.fallback.division,
                "type": groups.fallback.type,
                "gpu_hours": round(other["gpu_seconds"] / SECONDS_PER_HOUR, 2),
                "cpu_hours": round(other["cpu_seconds"] / SECONDS_PER_HOUR, 2),
                "jobs": other["jobs"],
                "users": len(other["users"]),
            }
        )

    return sorted(out, key=lambda e: (-e["gpu_hours"], e["name"]))


def _weighted_percentile(records: Iterable[dict], key: str) -> float | None:
    """Sample-count-weighted mean of a daily percentile.

    An approximation — the exact percentile over a month needs the full
    sample, which we deliberately do not retain. The methodology page says so
    explicitly rather than passing this off as exact.
    """
    total_weight = 0.0
    accumulated = 0.0
    for record in records:
        wait = record.get("wait_seconds") or {}
        value = wait.get(key)
        samples = wait.get("samples") or 0
        if value is None or samples <= 0:
            continue
        accumulated += value * samples
        total_weight += samples
    if total_weight <= 0:
        return None
    return round(accumulated / total_weight, 2)


def period_of(day: date, granularity: str) -> str:
    if granularity == "monthly":
        return f"{day.year:04d}-{day.month:02d}"
    return f"{day.year:04d}"


def build_rollup(
    day_records: list[dict],
    granularity: str,
    cluster: ClusterConfig,
    state: StateStore | None = None,
    groups: GroupsConfig | None = None,
    k_anonymity: int = 3,
) -> dict[str, Any]:
    """Aggregate day records into monthly or yearly periods."""
    buckets: dict[str, list[dict]] = {}
    for record in day_records:
        day = date.fromisoformat(record["date"])
        buckets.setdefault(period_of(day, granularity), []).append(record)

    periods: list[dict[str, Any]] = []
    for key in sorted(buckets):
        records = buckets[key]
        days = [date.fromisoformat(r["date"]) for r in records]

        allocated = round(_sum(records, "gpu_hours", "allocated"), 2)
        available = round(_sum(records, "gpu_hours", "available"), 2)
        reported = round(_sum(records, "gpu_hours", "reported"), 2)
        down = round(_sum(records, "gpu_hours", "down"), 2)

        entry: dict[str, Any] = {
            "period": key,
            "days": len(records),
            "gpu_hours": {
                "allocated": allocated,
                "available": available,
                "reported": reported,
                "down": down,
            },
            "cpu_hours_allocated": round(_sum(records, "cpu_hours_allocated"), 2),
            "utilization": {
                "available": round(allocated / available, 4) if available > 0 else None,
                "installed": round(allocated / reported, 4) if reported > 0 else None,
                "availability": round(available / reported, 4) if reported > 0 else None,
            },
            "jobs": {
                "total": int(_sum(records, "jobs", "total")),
                "by_state": _merge_counts(records, "jobs", "by_state"),
                "by_size": _merge_counts(records, "jobs", "by_size"),
            },
            "gpu_hours_by_model": _merge_maps(records, "gpu_hours_by_model"),
            "gpu_hours_by_partition": _merge_maps(records, "gpu_hours_by_partition"),
            "gpu_hours_by_qos": _merge_maps(records, "gpu_hours_by_qos"),
            "wait_seconds": {
                "p50": _weighted_percentile(records, "p50"),
                "p90": _weighted_percentile(records, "p90"),
                "p99": _weighted_percentile(records, "p99"),
                "approximate": True,
            },
            "peak_daily_gpu_hours": round(
                max((r["gpu_hours"]["allocated"] for r in records), default=0.0), 2
            ),
            "groups": _merge_groups(records),
        }

        # Prefer period-level k-anonymity from the account index; fall back to
        # merging the (already per-day suppressed) published records.
        if state is not None and groups is not None and days:
            period_groups = groups_for_period(state, groups, min(days), max(days), k_anonymity)
            if period_groups is not None:
                entry["groups"] = period_groups

        finished = sum(
            count
            for state_name, count in entry["jobs"]["by_state"].items()
            if state_name not in {"RUNNING", "PENDING", "SUSPENDED", "REQUEUED"}
        )
        entry["jobs"]["success_rate"] = (
            round(entry["jobs"]["by_state"].get("COMPLETED", 0) / finished, 4)
            if finished > 0
            else None
        )

        # True distinct users over the whole period, from the hashed index.
        # Summing daily counts would badly overcount a regular user.
        if state is not None and days:
            entry["unique_users"] = len(state.users_between(min(days), max(days)))
        else:
            entry["unique_users"] = None

        cost, basis = cost_avoided(
            entry["gpu_hours_by_model"],
            entry["gpu_hours"]["allocated"],
            cluster,
            max(days) if days else None,
        )
        if cost is not None:
            entry["cloud_cost_avoided_usd"] = cost
            entry["cloud_cost_basis"] = basis

        periods.append(entry)

    return {"granularity": granularity, "periods": periods}


def blended_usd_per_gpu_hour(cluster: ClusterConfig, day: date | None = None) -> float | None:
    """Installed-mix-weighted average on-demand rate for one point in time.

    Used when Slurm accounting cannot say WHICH model a GPU-hour was served by.
    This cluster's AccountingStorageTRES records `gres/gpu` with no typed
    variants, so every GPU-hour arrives model-less and the exact per-model join
    has nothing to match.

    This is an ESTIMATE and not a bound. It assumes demand is distributed across
    models in proportion to how many of each are installed, which is not true —
    the newest cards are usually the most contended, so this most likely
    UNDERSTATES. Callers must label it as an estimate; `cost_avoided` returns a
    basis string precisely so the page can.

    Returns None if any installed model lacks a price, rather than quietly
    averaging over a partial fleet and reporting a rate that is too low.
    """
    snapshot = (
        cluster.capacity_on(day)
        if day is not None
        else (cluster.capacity_timeline[-1] if cluster.capacity_timeline else None)
    )
    if snapshot is None:
        return None
    prices = cluster.priced_models()
    total_gpus = sum(snapshot.gpus.values())
    if total_gpus <= 0:
        return None
    if any(model not in prices for model in snapshot.gpus):
        return None
    weighted = sum(prices[model] * count for model, count in snapshot.gpus.items())
    return weighted / total_gpus


def cost_avoided(
    gpu_hours_by_model: dict[str, float],
    total_gpu_hours: float,
    cluster: ClusterConfig,
    day: date | None = None,
) -> tuple[float | None, str | None]:
    """Cost avoided plus the basis it was computed on.

    Prefers the exact per-model join. Falls back to the installed-mix blend
    when no GPU-hour can be attributed to a priced model. Returns
    (None, None) when the price table itself is not publishable, which keeps
    the existing "withhold rather than guess" rule intact for that case.
    """
    ok, _reason = cluster.pricing_is_publishable()
    if not ok:
        return None, None

    exact = estimate_cost_avoided(gpu_hours_by_model, cluster)
    if exact is not None:
        return exact, "per_model"

    rate = blended_usd_per_gpu_hour(cluster, day)
    if rate is None or total_gpu_hours <= 0:
        return None, None
    return round(total_gpu_hours * rate, 2), "blended"


def estimate_cost_avoided(
    gpu_hours_by_model: dict[str, float], cluster: ClusterConfig
) -> float | None:
    """GPU-hours x public on-demand rate.

    Returns None unless the price table is sourced and dated — an unsourced
    dollar figure on a public page invites exactly the challenge it is meant
    to survive. Unpriced or unknown models contribute zero rather than being
    silently valued at some other model's rate.
    """
    ok, _reason = cluster.pricing_is_publishable()
    if not ok:
        return None
    prices = cluster.priced_models()
    total = 0.0
    priced_hours = 0.0
    unpriced_hours = 0.0
    for model, hours in gpu_hours_by_model.items():
        rate = prices.get(model)
        if rate:
            total += hours * rate
            priced_hours += hours
        else:
            unpriced_hours += hours

    # This cluster's AccountingStorageTRES lists `gres/gpu` but no typed
    # variants, so every GPU-hour arrives keyed "unspecified" and none of it can
    # be priced. Falling through would publish $0 as a headline figure, which
    # reads as "this cluster saved nothing" rather than "we cannot price it".
    # Withhold instead, exactly as an unsourced price table is withheld.
    if priced_hours <= 0 and unpriced_hours > 0:
        return None
    return round(total, 2)


def build_records(
    day_records: list[dict],
    state: StateStore | None = None,
    max_job_hours: float = 0.0,
) -> dict[str, Any]:
    """All-time superlatives. The wall of headline numbers.

    `max_job_hours` rejects day records whose duration-derived maxima cannot
    describe a real job. This filter lives here, and not only at the source,
    for a specific reason: these values are read back out of ALREADY PUBLISHED
    day files, so a bad value baked into 2024-03.json would keep winning the
    all-time record on every future run. Applying the ceiling at read time
    heals the published history on the next ordinary run, with no need to
    re-query slurmdbd for three years of data.
    """
    if not day_records:
        return {"available": False}

    rejected = 0

    def ceiling_for(label: str, record: dict) -> float | None:
        """Upper bound for a metric on one day, or None if it is unbounded."""
        if max_job_hours <= 0:
            return None
        if label == "longest_job_hours":
            return max_job_hours
        if label == "largest_job_gpu_hours":
            # A job cannot accrue more GPU-hours than its own GPU count times
            # the longest it could have run. Using the same day's
            # largest_job_gpus keeps the bound tight and self-consistent
            # instead of guessing a cluster-wide maximum.
            gpus = (record.get("records") or {}).get("largest_job_gpus")
            if isinstance(gpus, (int, float)) and not isinstance(gpus, bool) and gpus > 0:
                return gpus * max_job_hours
            return None
        return None

    def best(path: tuple[str, ...], label: str) -> dict[str, Any] | None:
        nonlocal rejected
        winner = None
        for record in day_records:
            node: Any = record
            for key in path:
                node = (node or {}).get(key) if isinstance(node, dict) else None
            if not isinstance(node, (int, float)) or isinstance(node, bool):
                continue
            limit = ceiling_for(label, record)
            if limit is not None and node > limit:
                rejected += 1
                continue
            if winner is None or node > winner[1]:
                winner = (record["date"], node)
        if winner is None or winner[1] <= 0:
            return None
        return {"metric": label, "value": round(winner[1], 2), "date": winner[0]}

    entries = [
        best(("records", "largest_job_gpus"), "largest_job_gpus"),
        best(("records", "largest_job_gpu_hours"), "largest_job_gpu_hours"),
        best(("records", "longest_job_hours"), "longest_job_hours"),
        best(("records", "max_nodes_in_job"), "max_nodes_in_job"),
        best(("gpu_hours", "allocated"), "busiest_day_gpu_hours"),
        best(("jobs", "total"), "most_jobs_in_a_day"),
        best(("active_users",), "most_users_in_a_day"),
    ]

    total_gpu_hours = round(_sum(day_records, "gpu_hours", "allocated"), 2)
    dates = sorted(r["date"] for r in day_records)

    out: dict[str, Any] = {
        "available": True,
        "first_day": dates[0],
        "last_day": dates[-1],
        "days_observed": len(day_records),
        "total_gpu_hours": total_gpu_hours,
        "total_gpu_years": round(total_gpu_hours / HOURS_PER_YEAR, 2),
        "total_cpu_hours": round(_sum(day_records, "cpu_hours_allocated"), 2),
        "total_jobs": int(_sum(day_records, "jobs", "total")),
        "entries": [e for e in entries if e is not None],
        # Non-zero means published day records still contain impossible maxima
        # (orphaned RUNNING jobs). They are excluded from the wall above; a
        # re-collect of the affected months is what clears them at the source.
        "implausible_day_records_rejected": rejected,
    }

    if state is not None:
        first_seen = state.first_seen()
        out["researchers_all_time"] = len(first_seen)
    return out


def build_summary(
    day_records: list[dict],
    monthly: dict[str, Any],
    yearly: dict[str, Any],
    records: dict[str, Any],
    cluster: ClusterConfig,
    inventory: dict[str, Any] | None,
    state: StateStore | None = None,
    today: date | None = None,
    groups: GroupsConfig | None = None,
    k_anonymity: int = 3,
) -> dict[str, Any]:
    """The headline tiles, rendered server-side by Liquid at build time.

    Kept deliberately small and flat: this file is read by the template, not
    by JavaScript, so every value here should be directly displayable.
    """
    today = today or date.today()
    year_start = date(today.year, 1, 1)
    trailing_start = today - timedelta(days=365)

    ytd = [r for r in day_records if date.fromisoformat(r["date"]) >= year_start]
    trailing = [r for r in day_records if date.fromisoformat(r["date"]) >= trailing_start]

    ytd_allocated = round(_sum(ytd, "gpu_hours", "allocated"), 2)
    ytd_available = round(_sum(ytd, "gpu_hours", "available"), 2)
    ytd_reported = round(_sum(ytd, "gpu_hours", "reported"), 2)

    latest_capacity = cluster.capacity_timeline[-1] if cluster.capacity_timeline else None
    gpus_installed = latest_capacity.total_gpus if latest_capacity else None
    peak_pflops = cluster.peak_pflops(latest_capacity.gpus) if latest_capacity else None

    # Breadth of impact is judged over the trailing year, so k-anonymity is
    # applied against that period's distinct users rather than any one day's.
    trailing_groups: list[dict[str, Any]] | None = None
    if state is not None and groups is not None and trailing:
        days = [date.fromisoformat(r["date"]) for r in trailing]
        trailing_groups = groups_for_period(state, groups, min(days), max(days), k_anonymity)
    if trailing_groups is None:
        trailing_groups = _merge_groups(trailing)

    summary: dict[str, Any] = {
        "generated": None,  # stamped by publish.py
        "as_of": day_records[-1]["date"] if day_records else None,
        "gpus_installed": gpus_installed,
        "peak_pflops_fp16": round(peak_pflops, 1) if peak_pflops else None,
        "gpu_hours_ytd": ytd_allocated,
        "utilization_ytd": round(ytd_allocated / ytd_available, 4) if ytd_available > 0 else None,
        "utilization_ytd_installed": (
            round(ytd_allocated / ytd_reported, 4) if ytd_reported > 0 else None
        ),
        "availability_ytd": round(ytd_available / ytd_reported, 4) if ytd_reported > 0 else None,
        "jobs_ytd": int(_sum(ytd, "jobs", "total")),
        "success_rate_ytd": None,
        # Counts, not disclosures: how many labs/courses used the cluster, and
        # how many departments it serves. Neither names anybody. Populated
        # below from the account index where one exists.
        "labs_courses_trailing_year": None,
        "departments_served": len(groups.departments_served) if groups else None,
        "total_gpu_years": records.get("total_gpu_years"),
        "unique_users_trailing_year": None,
        "cloud_cost_avoided_ytd_usd": None,
        "cloud_cost_basis": None,
        "pricing_published": False,
    }

    by_state = _merge_counts(ytd, "jobs", "by_state")
    finished = sum(
        c for s, c in by_state.items() if s not in {"RUNNING", "PENDING", "SUSPENDED", "REQUEUED"}
    )
    if finished > 0:
        summary["success_rate_ytd"] = round(by_state.get("COMPLETED", 0) / finished, 4)

    if state is not None and trailing:
        days = [date.fromisoformat(r["date"]) for r in trailing]
        summary["unique_users_trailing_year"] = len(state.users_between(min(days), max(days)))

        if groups is not None and state.has_account_index():
            active = state.accounts_between(min(days), max(days))
            counted = {
                account
                for account in active
                if (entry := groups.classify(account)) is not None
                and entry.type in {"lab", "course", "clinic"}
            }
            summary["labs_courses_trailing_year"] = len(counted)

    cost, basis = cost_avoided(
        _merge_maps(ytd, "gpu_hours_by_model"),
        _sum(ytd, "gpu_hours", "allocated"),
        cluster,
        max((date.fromisoformat(r["date"]) for r in ytd), default=None) if ytd else None,
    )
    if cost is not None:
        summary["cloud_cost_avoided_ytd_usd"] = cost
        summary["cloud_cost_basis"] = basis
        summary["pricing_published"] = True

    if inventory:
        summary["nodes_online"] = inventory.get("nodes_online")
        summary["gpus_online"] = inventory.get("gpus_online")

    # Pre-formatted compact strings so the Liquid template can render the hero
    # tiles at BUILD time. Liquid has no thousands-separator filter, and the
    # tiles must be readable with JavaScript disabled — that is what makes the
    # headline numbers survive screenshots, PDF exports, and link previews.
    # Compact tokens ("4.2M") also read better on a tile than "4,231,884".
    summary["display"] = {
        "gpu_hours_ytd": compact(summary["gpu_hours_ytd"]),
        "utilization_ytd": percent(summary["utilization_ytd"]),
        "utilization_ytd_installed": percent(summary["utilization_ytd_installed"]),
        "availability_ytd": percent(summary["availability_ytd"]),
        "success_rate_ytd": percent(summary["success_rate_ytd"]),
        "jobs_ytd": compact(summary["jobs_ytd"]),
        "total_gpu_years": compact(summary["total_gpu_years"], digits=1),
        "unique_users_trailing_year": compact(summary["unique_users_trailing_year"]),
        "cloud_cost_avoided_ytd_usd": compact(summary["cloud_cost_avoided_ytd_usd"]),
        "peak_pflops_fp16": compact(summary["peak_pflops_fp16"], digits=1),
        "gpus_installed": compact(summary["gpus_installed"]),
    }

    # Storage totals are filled in by the storage source when it runs.
    return summary


def compact(value: float | int | None, digits: int = 1) -> str | None:
    """Human-scale number as a safe token: 4210000 -> '4.2M'.

    Deliberately free of commas and currency symbols so it passes the
    published-string vocabulary check unchanged; the template supplies '$'
    and units.
    """
    if value is None:
        return None
    number = float(value)
    for threshold, suffix in ((1e9, "B"), (1e6, "M"), (1e3, "k")):
        if abs(number) >= threshold:
            return f"{number / threshold:.{digits}f}{suffix}"
    if abs(number) >= 100 or float(number).is_integer():
        return f"{int(round(number))}"
    return f"{number:.{digits}f}"


def percent(value: float | None, digits: int = 1) -> str | None:
    """0.8734 -> '87.3'. The template supplies the % sign."""
    if value is None:
        return None
    return f"{value * 100:.{digits}f}"


def build_growth(day_records: list[dict], state: StateStore) -> dict[str, Any]:
    """New-user growth and retention cohorts, from the hashed user index."""
    first_seen = state.first_seen()
    if not first_seen:
        return {"available": False}

    new_by_month: dict[str, int] = {}
    for day in first_seen.values():
        key = f"{day.year:04d}-{day.month:02d}"
        new_by_month[key] = new_by_month.get(key, 0) + 1

    cohorts: dict[str, dict[str, int]] = {}
    for user, day in first_seen.items():
        cohorts.setdefault(f"{day.year:04d}-{day.month:02d}", {})[user] = 1

    retention: list[dict[str, Any]] = []
    known = state.known_days()
    if known:
        horizon = max(known)
        for cohort_key in sorted(cohorts):
            cohort_users = set(cohorts[cohort_key])
            cohort_start = date.fromisoformat(f"{cohort_key}-01")
            points = []
            for offset in (3, 6, 12):
                window_start = _add_months(cohort_start, offset)
                window_end = _add_months(window_start, 1) - timedelta(days=1)
                if window_start > horizon:
                    points.append(None)
                    continue
                active = state.users_between(window_start, min(window_end, horizon))
                points.append(round(len(cohort_users & active) / len(cohort_users), 4))
            retention.append(
                {
                    "cohort": cohort_key,
                    "size": len(cohort_users),
                    "month_3": points[0],
                    "month_6": points[1],
                    "month_12": points[2],
                }
            )

    return {
        "available": True,
        "new_users_by_month": dict(sorted(new_by_month.items())),
        "retention": retention,
    }


def _add_months(day: date, months: int) -> date:
    month_index = day.month - 1 + months
    year = day.year + month_index // 12
    month = month_index % 12 + 1
    return date(year, month, 1)
