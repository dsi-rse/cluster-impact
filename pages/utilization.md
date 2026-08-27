---
title: "Utilization"
permalink: /utilization/
nav_order: 1
---

{% assign s = site.data.summary %}
{% assign d = s.display %}

How much of the cluster's capacity turns into delivered compute, and how much
of it is idle or unavailable.

<span id="freshness" class="freshness freshness--unknown">Data freshness unknown</span>

<div class="stat-grid">
  {% include stat-tile.html label="Utilization (of available)" value=d.utilization_ytd unit="%" note="Year to date. Excludes hours when hardware was down or in maintenance." %}
  {% include stat-tile.html label="Utilization (of installed)" value=d.utilization_ytd_installed unit="%" note="Year to date, against every GPU-hour on the floor including downtime." %}
  {% include stat-tile.html label="Availability" value=d.availability_ytd unit="%" note="Share of installed GPU-hours that were up and schedulable." %}
  {% include stat-tile.html label="GPU-hours delivered" value=d.gpu_hours_ytd note="Year to date." %}
</div>

<div class="data-state data-state--info">
  <span class="data-state__title">Allocated, not measured</span>
  These figures describe GPU-hours <em>allocated</em> by the scheduler. Slurm
  knows a GPU was assigned to a job; it does not know whether that GPU was
  busy. Per-device utilization requires DCGM exporters, which are not yet
  deployed — see <a href="{{ '/methodology/' | relative_url }}">Methodology</a>.
  Nothing on this site claims measured device utilization until they are.
</div>

## Capacity over time

<div class="chart-card">
  <div class="chart-card__head">
    <h3 class="chart-card__title">GPU-hours per day</h3>
  </div>
  <p class="chart-card__sub">
    Stacked to total installed capacity: what was allocated, what sat idle,
    and what was unavailable.
  </p>
  <div id="chart-utilization" class="chart chart--tall"></div>
</div>

<div class="chart-card">
  <div class="chart-card__head">
    <h3 class="chart-card__title">Utilization and availability rates</h3>
  </div>
  <div id="chart-rate" class="chart"></div>
</div>

## Where the hours go

<div class="data-state data-state--info">
  <span class="data-state__title">Demand by GPU model is not available</span>
  Slurm's accounting on this cluster records that a job used a GPU, but not
  which model — <code>AccountingStorageTRES</code> tracks <code>gres/gpu</code>
  with no per-model breakdown, so every GPU-hour arrives unattributed. Jobs
  <em>can</em> request a specific model and the scheduler honours it; the type
  simply is not retained in the accounting record. Until that changes there is
  no honest way to chart demand per generation, so nothing is charted here.
  The installed mix is on <a href="{{ '/capacity/' | relative_url }}">Capacity</a>.
</div>

<div class="chart-card">
  <div class="chart-card__head">
    <h3 class="chart-card__title">By partition</h3>
  </div>
  <div id="chart-partitions" class="chart"></div>
</div>

<div class="chart-card">
  <div class="chart-card__head">
    <h3 class="chart-card__title">By quality of service</h3>
  </div>
  <p class="chart-card__sub">
    The cluster runs three QoS tiers — <code>general</code>,
    <code>protected</code>, and <code>interactive</code>. Preemptible work
    running in gaps is capacity that would otherwise have been wasted.
  </p>
  <div id="chart-qos" class="chart"></div>
</div>

<div class="chart-card">
  <div class="chart-card__head">
    <h3 class="chart-card__title">Usage by hour and weekday</h3>
  </div>
  <p class="chart-card__sub">Mean GPU-hours allocated, last 90 days.</p>
  <div id="chart-heatmap" class="chart"></div>
</div>

<script>
  document.addEventListener('DOMContentLoaded', function () {
    Impact.freshness('#freshness');
    Impact.utilizationTrend('chart-utilization', { days: 365 });
    Impact.utilizationRate('chart-rate', { days: 365 });
    Impact.stackedByKey('chart-partitions', 'gpu_hours_by_partition', { days: 180 });
    Impact.stackedByKey('chart-qos', 'gpu_hours_by_qos', { days: 180 });
    Impact.hourHeatmap('chart-heatmap', { days: 90 });
  });
</script>
