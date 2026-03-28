from __future__ import annotations

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]

REQUIRED_FILES = [
    "README.md",
    "dbt_project.yml",
    "profiles.example.yml",
    "requirements.txt",
    "docs/architecture.md",
    "models/sources.yml",
    "models/schema.yml",
    "models/staging/stg_eth_blocks.sql",
    "models/staging/stg_eth_transactions.sql",
    "models/staging/stg_eth_receipts.sql",
    "models/staging/stg_eth_token_transfers.sql",
    "models/staging/stg_eth_accounts.sql",
    "models/staging/stg_monitored_assets.sql",
    "models/marts/fct_daily_network_activity.sql",
    "models/marts/fct_stablecoin_daily_flows.sql",
    "models/marts/fct_stablecoin_network_share.sql",
    "models/marts/dim_stablecoin_wallet_activity.sql",
    "queries/export_daily_network_activity.sql",
    "queries/export_stablecoin_daily_flows.sql",
    "queries/export_stablecoin_network_share.sql",
    "queries/export_stablecoin_wallet_activity.sql",
    "queries/ad_hoc_top_usdc_holders.sql",
    "queries/ad_hoc_stablecoin_wallet_concentration.sql",
    "queries/ad_hoc_latest_mint_burn_days.sql",
    "scripts/run_bigquery_exports.py",
    "scripts/build_llm_brief.py",
    "streamlit_app.py",
    ".streamlit/config.toml",
    "seeds/monitored_assets.csv",
]


def main() -> None:
    missing = [path for path in REQUIRED_FILES if not (PROJECT_ROOT / path).exists()]
    if missing:
        raise SystemExit("Missing required files:\n- " + "\n- ".join(missing))
    print(f"validated {len(REQUIRED_FILES)} required files")


if __name__ == "__main__":
    main()
