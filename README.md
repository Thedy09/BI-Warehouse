# Stablecoin Intelligence Warehouse

An Analytics Engineering portfolio project for blockchain business intelligence.

This repository models real Ethereum stablecoin activity in BigQuery with `dbt`, then exposes the resulting marts through an interactive Streamlit dashboard and an AI-ready analyst brief. The focus is operational monitoring for USDC, USDT, and DAI rather than notebook-style exploration.

## What this project shows

- Warehouse-first analytics engineering with layered `dbt` models
- Real blockchain data modeling on top of Google Cloud public datasets
- Decision-support marts for flows, issuance, network share, and wallet activity
- A live reporting layer in Streamlit that reads directly from BigQuery
- Python support scripts for exports and AI-assisted brief generation

## Stack

- BigQuery
- dbt
- Python
- Streamlit
- Plotly
- GitHub Actions

## Business use case

The project is shaped like a BI team deliverable for stablecoin monitoring:

- How much stablecoin volume moved today?
- Which issuer or asset drove the change?
- What share of Ethereum activity came from tracked stablecoins?
- Which wallets deserve analyst review because of unusual net flow or concentration?

## Data sources

The source data comes from the Google Cloud public Ethereum dataset:

- `bigquery-public-data.goog_blockchain_ethereum_mainnet_us.blocks`
- `bigquery-public-data.goog_blockchain_ethereum_mainnet_us.transactions`
- `bigquery-public-data.goog_blockchain_ethereum_mainnet_us.receipts`
- `bigquery-public-data.goog_blockchain_ethereum_mainnet_us.token_transfers`
- `bigquery-public-data.goog_blockchain_ethereum_mainnet_us.accounts`

Tracked assets are defined in [`seeds/monitored_assets.csv`](seeds/monitored_assets.csv). The repository contains real contract metadata only. No synthetic blockchain events are stored in the repo.

## Architecture

```text
Google public blockchain tables
        ->
dbt staging models
        ->
dbt marts for stablecoin intelligence
        ->
Streamlit dashboard + analyst brief
```

More detail is available in [`docs/architecture.md`](docs/architecture.md).

## Core models

- [`models/marts/fct_daily_network_activity.sql`](models/marts/fct_daily_network_activity.sql): network-level daily activity and gas metrics
- [`models/marts/fct_stablecoin_daily_flows.sql`](models/marts/fct_stablecoin_daily_flows.sql): per-stablecoin daily flows, mint, burn, and active wallet metrics
- [`models/marts/fct_stablecoin_network_share.sql`](models/marts/fct_stablecoin_network_share.sql): stablecoin share of Ethereum transaction activity
- [`models/marts/dim_stablecoin_wallet_activity.sql`](models/marts/dim_stablecoin_wallet_activity.sql): wallet-level inflow, outflow, net flow, and contract flag

## Dashboard

The dashboard lives in [`streamlit_app.py`](streamlit_app.py) and prefers live BigQuery reads.

It supports:

- direct BigQuery loading from the sidebar
- date range filtering
- asset filtering across USDC, USDT, and DAI
- wallet segment filtering for EOAs vs contracts
- top-wallet ranking by net flow, inflow, outflow, or transaction count
- an auto-generated ops brief and JSON context pack

## Repository structure

```text
.
├── models/
├── queries/
├── scripts/
├── seeds/
├── docs/
├── .github/workflows/
├── streamlit_app.py
├── dbt_project.yml
├── profiles.example.yml
└── requirements.txt
```

## Local setup

1. Create an environment and install dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2. Copy [`profiles.example.yml`](profiles.example.yml) to `~/.dbt/profiles.yml` and update your GCP settings.

3. Export runtime variables:

```bash
export GCP_PROJECT_ID=your-gcp-project
export BIGQUERY_DATASET=onchain_intelligence
export BIGQUERY_LOCATION=US
export GOOGLE_APPLICATION_CREDENTIALS=/absolute/path/to/service-account.json
```

4. Build the warehouse models:

```bash
dbt seed
dbt run --vars '{"lookback_days": 30}'
dbt test
```

5. Launch the dashboard:

```bash
streamlit run streamlit_app.py
```

In the sidebar, leave `Mode = BigQuery`, enter your project and dataset, then click `Load from BigQuery`.

## Optional export workflow

If you want local CSV artifacts or a portable analyst brief, run:

```bash
python3 scripts/run_bigquery_exports.py \
  --project "$GCP_PROJECT_ID" \
  --dataset "$BIGQUERY_DATASET" \
  --output-dir artifacts

python3 scripts/build_llm_brief.py \
  --input-dir artifacts \
  --output-dir artifacts
```

## Source references

- Google Cloud Blockchain Analytics:
  https://docs.cloud.google.com/blockchain-analytics/docs/dataset-ethereum
  https://docs.cloud.google.com/blockchain-analytics/docs/example-ethereum
- dbt BigQuery setup:
  https://docs.getdbt.com/docs/local/connect-data-platform/bigquery-setup
- Circle USDC contract addresses:
  https://developers.circle.com/stablecoins/usdc-contract-addresses
- Tether supported protocols:
  https://tether.to/en/supported-protocols/

## License

MIT. See [`LICENSE`](LICENSE).

