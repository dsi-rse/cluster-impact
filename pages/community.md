---
title: "Community"
permalink: /community/
nav_order: 2
---

{% assign s = site.data.summary %}
{% assign d = s.display %}

Who the cluster serves. This is the measure that distinguishes shared
institutional infrastructure from one lab's private machine.

<span id="freshness" class="freshness freshness--unknown">Data freshness unknown</span>

<div class="stat-grid">
  {% include stat-tile.html label="Researchers" value=d.unique_users_trailing_year note="Distinct people who ran at least one job in the last 12 months." %}
  {% include stat-tile.html label="Labs & courses" value=s.labs_named_trailing_year note="Named groups meeting the anonymity threshold." %}
  {% include stat-tile.html label="Departments" value=s.departments_trailing_year note="Distinct departments represented." %}
</div>

<div class="data-state data-state--info">
  <span class="data-state__title">Individuals are never named</span>
  This site publishes counts and group-level totals only. A research group is
  named only when an operator has explicitly added it to the site's allowlist
  <em>and</em> it has at least three distinct users in the period shown.
  Everything else is aggregated into “Other” — still counted, never
  identified. See <a href="{{ '/methodology/' | relative_url }}">Methodology</a>.
</div>

## Growth

<div class="chart-card">
  <div class="chart-card__head">
    <h3 class="chart-card__title">New researchers per month</h3>
  </div>
  <p class="chart-card__sub">
    Bars are people running their first job that month; the line is the
    cumulative total who have ever used the cluster.
  </p>
  <div id="chart-growth" class="chart"></div>
</div>

<div class="chart-card">
  <div class="chart-card__head">
    <h3 class="chart-card__title">Distinct researchers per month</h3>
  </div>
  <p class="chart-card__sub">
    Counted once per month regardless of how often they ran — this is people,
    not sessions.
  </p>
  <div id="chart-monthly-users" class="chart"></div>
</div>

<div class="chart-card">
  <div class="chart-card__head">
    <h3 class="chart-card__title">Active researchers per day</h3>
  </div>
  <div id="chart-daily-users" class="chart chart--short"></div>
</div>

## Breadth of use

<div class="chart-card">
  <div class="chart-card__head">
    <h3 class="chart-card__title">GPU-hours by group</h3>
  </div>
  <p class="chart-card__sub">Most recent full year.</p>
  <div id="chart-groups" class="chart chart--tall"></div>
</div>

<script>
  document.addEventListener('DOMContentLoaded', function () {
    Impact.freshness('#freshness');
    Impact.newUsers('chart-growth');
    Impact.monthlyUniqueUsers('chart-monthly-users');
    Impact.activeUsers('chart-daily-users', { days: 180 });
    Impact.groupBreakdown('chart-groups', { granularity: 'yearly', top: 15 });
  });
</script>
