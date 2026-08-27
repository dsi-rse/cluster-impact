---
title: "Methodology"
permalink: /methodology/
nav_order: 7
toc: true
toc_sticky: true
---

Every number on this site should be reproducible by someone with a shell on
the cluster. This page says exactly how each one is computed, and — more
importantly — what it cannot tell you.

## Where the data comes from

A collector runs nightly in a container on a DSI cluster node. It queries
Slurm accounting, the storage nodes, the account directory, and Prometheus;
aggregates the results to one record per calendar day; removes all identifying
detail; and pushes the result to this site's public repository. GitHub Actions
then rebuilds the site.

Raw job records never leave the cluster. The repository contains only
aggregated, scrubbed daily figures.

## GPU-hours

Per-job GPU-hours come from `sacct`:

```
sacct --allusers --allocations --parsable2 --noheader \
      --start <day>T00:00:00 --end <day+1>T00:00:00 \
      --format=JobIDRaw,User,Account,Partition,QOS,State,Submit,Start,End,\
ElapsedRaw,AllocTRES,NNodes,NCPUS,ExitCode
```

GPU count is parsed from `AllocTRES`. When Slurm reports both a plain
`gres/gpu` total and typed `gres/gpu:<model>` entries, the plain value is
authoritative for the total and the typed values give the model breakdown —
adding both would double-count.

**Jobs spanning midnight are split across days** in proportion to the seconds
they occupied on each. A thirty-hour job is not thirty GPU-hours on its start
date. Job *counts*, by contrast, are attributed entirely to the day the job
started, so a job is never counted twice.

Jobs still running when the snapshot is taken are clipped to the window edge.
Because of this — and because Slurm's accounting database settles late — every
run recomputes the **trailing seven days**, not just yesterday.

## Utilization: the two denominators

This is the number most easily massaged, so both versions are always
published.

The denominator comes from `sreport`, which reports the allocated / idle /
down / planned split directly:

```
sreport --parsable2 --noheader -t Seconds -T gres/gpu \
        cluster utilization start=<day> end=<day+1>
```

| Figure | Definition |
| --- | --- |
| **Utilization (of available)** | Allocated ÷ (Reported − Down − Planned Down). The headline. Measures how well the scheduler fills the capacity it actually had. |
| **Utilization (of installed)** | Allocated ÷ Reported. Counts downtime against us. Always the lower number. |
| **Availability** | (Reported − Down − Planned Down) ÷ Reported. Published separately so the excluded time is visible rather than hidden. |

Leading with the available-capacity figure and publishing the other two
alongside it is a deliberate choice: it is the fairest measure of scheduling
efficiency, and printing the other two means nothing is being concealed by the
choice.

For historical days where `sreport` data is unavailable, the denominator falls
back to the capacity timeline in `config/cluster.yaml`. That path cannot
distinguish downtime from idleness, so it reports available = installed, and
those days are flagged `from_sreport: false` in the published JSON.

### Capacity changes over time

`config/cluster.yaml` records when each group of GPUs entered service. The
denominator for any given day uses only the hardware installed on that day.
Without this, adding new nodes would retroactively depress every historical
utilization figure.

## Allocated is not utilized

**Slurm can only tell us a GPU was assigned to a job. It cannot tell us the
GPU was busy.** A job holding eight H100s and using one of them looks
identical to a job saturating all eight.

Everything on this site therefore says *allocated*. Measured device
utilization requires DCGM exporters, which are not yet deployed on this
cluster. The collector and the site panels for SM-activity, memory
high-water, power draw, and energy already exist and are switched off; they
turn on when the exporters land.

Any HPC dashboard that reports "GPU utilization" without per-device telemetry
is reporting allocation. This one says so.

## Privacy

The repository is public, so the privacy model is enforced as a build gate
rather than a convention.

1. **Allowlist.** A Slurm account is named only if an operator has explicitly
   added it to `config/groups.yaml`. Anything else is published as “Other” —
   still counted in every total, never named.
2. **k-anonymity.** A named group must have at least **3 distinct users** in
   the period shown. A one-person lab is a person, so naming it names them; it
   collapses into “Other” regardless of the allowlist.
3. **Assertion.** Before anything is committed, and again in CI before the
   site is built, every published file is re-read and checked against a closed
   schema with a closed string vocabulary. Unknown keys, unknown group names,
   buckets below the threshold, and any free-text string all fail the build.
   The deploy job depends on this check, so data that fails it cannot reach
   the site.

The threshold is applied at the granularity being published. A course whose
three students never run on the same day is anonymous in the daily series and
named in the monthly rollup — it is genuinely anonymous at monthly
granularity, and suppressing it there would protect nobody while erasing real
breadth of use.

Some things are never collected at all, which is stronger than filtering them
out later: **job names are never requested from `sacct`**, because on a
research cluster they routinely contain grant numbers, subject identifiers,
and unpublished paper titles. Job IDs, exit codes, and node names are also
absent from published data. Usernames used for counting are hashed with a
machine-local secret before ever touching disk, on the cluster, in a directory
that is not part of this repository.

## Queue wait

Wait is `Start − Submit` for jobs that started. Jobs that never started
contribute no sample. Daily percentiles are exact.

**Monthly and yearly percentiles are approximate**: they are sample-count
weighted means of the daily percentiles, because the full sample is
deliberately not retained. They are labelled `approximate: true` in the
published JSON. Treat monthly p99 as indicative, not exact.

## Job success rate

Completed ÷ (all jobs in a terminal state). `RUNNING`, `PENDING`,
`SUSPENDED`, and `REQUEUED` are excluded from both sides. `CANCELLED` counts
against the rate, which arguably overstates failure — a user cancelling their
own job is not a cluster fault — so read it alongside the outcome breakdown on
the [Responsiveness]({{ '/responsiveness/' | relative_url }}) page.

## Peak PFLOPS

Sum over installed GPUs of the vendor's **dense** FP16 tensor-core figure. No
2:4 sparsity multiplier is applied, though vendor sheets usually quote the
sparse number first — using it would roughly double the headline for no honest
reason.

This is a theoretical ceiling describing the hardware, not a benchmark result.
No real workload reaches it.

## Cloud cost avoided

Allocated GPU-hours × a public on-demand list rate. The rate table, its
source, and its retrieval date are published on the
[Value Delivered]({{ '/value/' | relative_url }}) page, which also explains at
length what the figure does and does not mean. The collector refuses to
compute it at all unless a source and an as-of date are recorded.

**It is currently an estimate, not an exact figure.** The exact calculation is
GPU-hours *per model* × that model's rate, but Slurm's accounting on this
cluster records `gres/gpu` with no per-model breakdown
(`AccountingStorageTRES`), so a GPU-hour cannot be attributed to a specific
card. Each period's hours are therefore priced at the average rate of the
fleet **installed in that period**, weighted by unit counts. The published
figure carries a basis of `blended` to mark this; it would read `per_model` if
typed accounting were ever enabled. Because the newest and most expensive
cards are also the most contended, this assumption most likely understates.

## Counting groups and departments

Two figures on [Community]({{ '/community/' | relative_url }}) are counts of
groups, and they are computed differently from each other on purpose.

**Labs & courses** is measured. It counts the Slurm accounts classified as a
research group, course, or clinic that ran at least one job in the last twelve
months. A group is counted whether or not it can be *named* — naming requires
both an operator's explicit decision and the k-anonymity floor, while counting
requires neither and discloses nothing. A one-person lab is therefore counted
but never identified.

**Departments** is declared, not measured. Slurm records no department for a
job or an account, so unlike every other number on this site it comes from a
list maintained by the cluster operators in `config/groups.yaml` rather than
from the data. It is published because it is useful and true, and labelled
here because it is a different kind of claim.

## Storage

Capacity is read with `zfs list -Hp` on the storage nodes. I/O throughput,
when published, is integrated from Prometheus rate series rather than from
`zpool iostat`, whose cumulative counters reset on reboot and would produce
meaningless deltas.

## Known gaps

- **No measured GPU utilization** until DCGM exporters are deployed.
- **No energy or carbon figures** for the same reason.
- **History reaches only as far back as the accounting database retains.**
  Aggregates are committed to git precisely so this horizon stops receding —
  but data purged before this site existed is gone.
- **Monthly and yearly wait percentiles are approximations** (see above).
- **Storage capacity and I/O are both unavailable.** Capacity needs root on
  the storage nodes, and I/O needs Prometheus; as of 2026-08 the collector host
  can reach neither (`s10:9090` is firewalled in the collector's direction, and
  `root@builder` refuses its key). Both panels show "unavailable" rather than a
  stale or invented number.
- **Demand by GPU model cannot be charted** for the same accounting reason
  described under Cloud cost avoided: the model that served a GPU-hour is not
  recorded.
- **Publication and grant outcomes** are not tracked here. They are the
  metric that matters most and the one this tooling cannot reach.

## Reproducing these numbers

Every figure is derived from the JSON published in this repository, which you
can [download]({{ '/data-access/' | relative_url }}). The collector source is
in the same repository. If a number here disagrees with what you get from
`sreport` for the same window, that is a bug — please report it.
