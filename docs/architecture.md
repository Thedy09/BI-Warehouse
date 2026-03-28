# Architecture

## Goal

Turn public Ethereum blockchain tables into decision-ready datasets that a BI team can use for stablecoin monitoring, reporting, and AI-assisted analysis.

## Proposed flow

1. Public on-chain data lands in BigQuery through Google Cloud public datasets.
2. `dbt` models clean and reshape raw blockchain structures into analytics-friendly tables.
3. Python export scripts materialize selected outputs into CSV artifacts for lightweight sharing and downstream AI tooling.
4. A briefing script packages recent changes into a Markdown summary and a machine-readable JSON context bundle.

## Layering

### Sources

- Ethereum blocks
- Ethereum transactions
- Ethereum receipts
- Ethereum token transfers
- Ethereum accounts
- A small stablecoin reference seed with real contract metadata

### Staging

The staging layer standardizes addresses, timestamps, numeric types, and recent filtering logic.

### Marts

- Daily network health
- Stablecoin daily flow monitoring
- Stablecoin share of network activity
- Stablecoin wallet monitoring

## Why this is a good portfolio shape

- It looks like a real analytics engineering repo instead of a one-off notebook.
- It demonstrates business-facing marts, not just raw exploration.
- It shows how Python can support analytics operations without replacing SQL modeling.
- It leaves room for production extensions such as labeled wallet reference sources, Looker Studio dashboards, and AI-based triage or anomaly explanations.
