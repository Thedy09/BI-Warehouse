from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from statistics import mean


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"Missing input file: {path}")
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def to_float(value: str | None) -> float:
    if value in (None, "", "None"):
        return 0.0
    return float(value)


def safe_mean(values: list[float]) -> float:
    return mean(values) if values else 0.0


def build_summary(
    daily_network: list[dict[str, str]],
    stablecoin_flows: list[dict[str, str]],
    network_share: list[dict[str, str]],
    wallets: list[dict[str, str]],
) -> dict[str, object]:
    latest_network = daily_network[0] if daily_network else {}
    prior_network = daily_network[1:8]
    latest_share = network_share[0] if network_share else {}
    prior_share = network_share[1:8]

    tx_count = to_float(latest_network.get("transaction_count"))
    avg_prior_tx_count = safe_mean([to_float(row.get("transaction_count")) for row in prior_network])
    gas_paid = to_float(latest_network.get("total_gas_paid_eth"))
    avg_prior_gas_paid = safe_mean([to_float(row.get("total_gas_paid_eth")) for row in prior_network])
    stablecoin_tx_share = to_float(latest_share.get("share_of_network_transactions"))
    avg_prior_tx_share = safe_mean([to_float(row.get("share_of_network_transactions")) for row in prior_share])

    latest_day = latest_share.get("block_date") or latest_network.get("block_date")
    latest_asset_rows = [row for row in stablecoin_flows if row.get("block_date") == latest_day]

    top_wallets = [
        {
            "asset_symbol": row.get("asset_symbol"),
            "wallet_address": row.get("wallet_address"),
            "issuer": row.get("issuer"),
            "net_flow": to_float(row.get("net_flow")),
            "transaction_count": int(to_float(row.get("transaction_count"))),
        }
        for row in wallets[:5]
    ]

    return {
        "latest_day": latest_day,
        "network": {
            "transaction_count": tx_count,
            "avg_prior_7d_transaction_count": avg_prior_tx_count,
            "transaction_delta_vs_prior_7d": tx_count - avg_prior_tx_count,
            "gas_paid_eth": gas_paid,
            "avg_prior_7d_gas_paid_eth": avg_prior_gas_paid,
            "gas_delta_vs_prior_7d": gas_paid - avg_prior_gas_paid,
        },
        "stablecoin_network_share": {
            "share_of_network_transactions": stablecoin_tx_share,
            "avg_prior_7d_share_of_network_transactions": avg_prior_tx_share,
            "share_delta_vs_prior_7d": stablecoin_tx_share - avg_prior_tx_share,
            "stablecoin_transaction_count": to_float(latest_share.get("stablecoin_transaction_count")),
            "stablecoin_quantity_transferred": to_float(latest_share.get("stablecoin_quantity_transferred")),
        },
        "asset_highlights": [
            {
                "asset_symbol": row.get("asset_symbol"),
                "issuer": row.get("issuer"),
                "quantity_transferred": to_float(row.get("quantity_transferred")),
                "net_issuance": to_float(row.get("net_issuance")),
                "active_wallet_count": int(to_float(row.get("active_wallet_count"))),
            }
            for row in latest_asset_rows
        ],
        "top_wallets": top_wallets,
    }


def build_markdown(summary: dict[str, object]) -> str:
    network = summary["network"]
    stablecoin_network_share = summary["stablecoin_network_share"]
    asset_highlights = summary["asset_highlights"]
    top_wallets = summary["top_wallets"]

    asset_lines = []
    for asset in asset_highlights:
        asset_lines.append(
            f"- {asset['asset_symbol']} ({asset['issuer']}): transferred {asset['quantity_transferred']:.2f}, "
            f"net issuance {asset['net_issuance']:.2f}, active wallets {asset['active_wallet_count']}"
        )

    wallet_lines = []
    for wallet in top_wallets:
        wallet_lines.append(
            f"- {wallet['asset_symbol']} {wallet['wallet_address']}: net flow {wallet['net_flow']:.2f}, "
            f"transaction count {wallet['transaction_count']}"
        )

    return "\n".join(
        [
            "# Stablecoin Ops Brief",
            "",
            f"Latest day covered: {summary['latest_day']}",
            "",
            "## Network summary",
            f"- Transactions: {network['transaction_count']:.0f}",
            f"- Delta vs prior 7-day average: {network['transaction_delta_vs_prior_7d']:.2f}",
            f"- Gas paid (ETH): {network['gas_paid_eth']:.4f}",
            f"- Gas delta vs prior 7-day average: {network['gas_delta_vs_prior_7d']:.4f}",
            "",
            "## Stablecoin network share",
            f"- Stablecoin share of network transactions: {stablecoin_network_share['share_of_network_transactions']:.4f}",
            f"- Delta vs prior 7-day average: {stablecoin_network_share['share_delta_vs_prior_7d']:.4f}",
            f"- Stablecoin transaction count: {stablecoin_network_share['stablecoin_transaction_count']:.0f}",
            f"- Stablecoin quantity transferred: {stablecoin_network_share['stablecoin_quantity_transferred']:.2f}",
            "",
            "## Asset highlights",
            *asset_lines,
            "",
            "## Wallets to review",
            *wallet_lines,
            "",
            "## Suggested analyst prompts",
            "- Which issuer or stablecoin explains the largest share of the latest activity spike?",
            "- Did network usage rise because stablecoin transaction share increased or because broader activity increased?",
            "- Which high-net-flow wallets should be labeled as exchanges, bridges, treasuries, or protocol contracts?",
        ]
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a Markdown and JSON briefing pack from exported CSVs.")
    parser.add_argument("--input-dir", default="artifacts", help="Directory containing CSV exports.")
    parser.add_argument("--output-dir", default="artifacts", help="Directory for generated brief files.")
    args = parser.parse_args()

    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    daily_network = read_csv(input_dir / "daily_network_activity.csv")
    stablecoin_flows = read_csv(input_dir / "stablecoin_daily_flows.csv")
    network_share = read_csv(input_dir / "stablecoin_network_share.csv")
    wallets = read_csv(input_dir / "stablecoin_wallet_activity.csv")

    summary = build_summary(daily_network, stablecoin_flows, network_share, wallets)
    markdown = build_markdown(summary)

    (output_dir / "llm_context.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    (output_dir / "weekly_brief.md").write_text(markdown + "\n", encoding="utf-8")

    print(f"wrote {output_dir / 'llm_context.json'}")
    print(f"wrote {output_dir / 'weekly_brief.md'}")


if __name__ == "__main__":
    main()
