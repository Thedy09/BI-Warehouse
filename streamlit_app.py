from __future__ import annotations

import json
import os
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

from scripts.build_llm_brief import build_markdown, build_summary


PROJECT_ROOT = Path(__file__).resolve().parent
ARTIFACTS_DIR = PROJECT_ROOT / "artifacts"
QUERY_FILES = {
    "daily_network_activity": PROJECT_ROOT / "queries" / "export_daily_network_activity.sql",
    "stablecoin_daily_flows": PROJECT_ROOT / "queries" / "export_stablecoin_daily_flows.sql",
    "stablecoin_network_share": PROJECT_ROOT / "queries" / "export_stablecoin_network_share.sql",
    "stablecoin_wallet_activity": PROJECT_ROOT / "queries" / "export_stablecoin_wallet_activity.sql",
}
LOCAL_FILES = {
    "daily_network_activity": ARTIFACTS_DIR / "daily_network_activity.csv",
    "stablecoin_daily_flows": ARTIFACTS_DIR / "stablecoin_daily_flows.csv",
    "stablecoin_network_share": ARTIFACTS_DIR / "stablecoin_network_share.csv",
    "stablecoin_wallet_activity": ARTIFACTS_DIR / "stablecoin_wallet_activity.csv",
}
PALETTE = {
    "USDC": "#0f766e",
    "USDT": "#2563eb",
    "DAI": "#c2410c",
}


st.set_page_config(
    page_title="Stablecoin Intelligence Warehouse",
    page_icon=":bar_chart:",
    layout="wide",
    initial_sidebar_state="expanded",
)


def inject_css() -> None:
    st.markdown(
        """
        <style>
        .stApp {
            background:
                radial-gradient(circle at top left, rgba(15, 118, 110, 0.10), transparent 32%),
                radial-gradient(circle at top right, rgba(37, 99, 235, 0.08), transparent 28%),
                linear-gradient(180deg, #f6f3ea 0%, #f2eee3 100%);
        }
        .block-container {
            padding-top: 2.2rem;
            padding-bottom: 2rem;
        }
        .hero {
            padding: 1.35rem 1.5rem;
            border: 1px solid rgba(19, 35, 47, 0.10);
            border-radius: 22px;
            background: linear-gradient(135deg, rgba(255,255,255,0.92), rgba(232,226,211,0.78));
            box-shadow: 0 10px 30px rgba(19, 35, 47, 0.06);
            margin-bottom: 1.2rem;
        }
        .hero-kicker {
            text-transform: uppercase;
            letter-spacing: 0.14em;
            font-size: 0.72rem;
            color: #0f766e;
            margin-bottom: 0.35rem;
            font-weight: 600;
        }
        .hero-title {
            font-size: 2.2rem;
            line-height: 1.05;
            color: #13232f;
            margin: 0 0 0.45rem 0;
        }
        .hero-subtitle {
            font-size: 1rem;
            color: rgba(19, 35, 47, 0.78);
            margin: 0;
            max-width: 920px;
        }
        .source-chip {
            display: inline-block;
            margin-top: 0.75rem;
            padding: 0.35rem 0.6rem;
            border-radius: 999px;
            background: rgba(19, 35, 47, 0.06);
            color: #13232f;
            font-size: 0.82rem;
        }
        .metric-card {
            border: 1px solid rgba(19, 35, 47, 0.10);
            border-radius: 18px;
            background: rgba(255,255,255,0.80);
            padding: 0.95rem 1rem;
            min-height: 122px;
            box-shadow: 0 8px 22px rgba(19, 35, 47, 0.04);
        }
        .metric-label {
            font-size: 0.78rem;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            color: rgba(19, 35, 47, 0.62);
            margin-bottom: 0.5rem;
        }
        .metric-value {
            font-size: 1.9rem;
            line-height: 1;
            color: #13232f;
            margin-bottom: 0.35rem;
        }
        .metric-delta {
            font-size: 0.92rem;
            color: #0f766e;
        }
        .section-title {
            font-size: 1.15rem;
            color: #13232f;
            margin: 0.4rem 0 0.8rem 0;
            letter-spacing: 0.02em;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_query(sql_path: Path, project: str, dataset: str) -> str:
    raw_sql = sql_path.read_text(encoding="utf-8")
    return raw_sql.replace("{{ target_project }}", project).replace("{{ target_dataset }}", dataset).strip()


@st.cache_data(show_spinner=False)
def load_local_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path)


@st.cache_data(show_spinner="Chargement des datasets depuis BigQuery...", ttl=900)
def load_bigquery_frames(project: str, dataset: str, location: str) -> dict[str, pd.DataFrame]:
    from google.cloud import bigquery

    client = bigquery.Client(project=project, location=location)
    frames: dict[str, pd.DataFrame] = {}
    for name, sql_path in QUERY_FILES.items():
        query = render_query(sql_path, project, dataset)
        frames[name] = client.query(query).to_dataframe(create_bqstorage_client=False)
    return frames


def load_local_frames() -> dict[str, pd.DataFrame] | None:
    if any(not path.exists() for path in LOCAL_FILES.values()):
        return None
    return {name: load_local_csv(path) for name, path in LOCAL_FILES.items()}


def load_uploaded_frames() -> dict[str, pd.DataFrame] | None:
    st.sidebar.markdown("**CSV requis**")
    uploads = {
        "daily_network_activity": st.sidebar.file_uploader("daily_network_activity.csv", type="csv", key="upload_network"),
        "stablecoin_daily_flows": st.sidebar.file_uploader("stablecoin_daily_flows.csv", type="csv", key="upload_flows"),
        "stablecoin_network_share": st.sidebar.file_uploader("stablecoin_network_share.csv", type="csv", key="upload_share"),
        "stablecoin_wallet_activity": st.sidebar.file_uploader("stablecoin_wallet_activity.csv", type="csv", key="upload_wallets"),
    }
    if not all(uploads.values()):
        return None
    return {name: pd.read_csv(file) for name, file in uploads.items()}


def prepare_dataframes(frames: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    prepared = {name: frame.copy() for name, frame in frames.items()}

    prepared["daily_network_activity"]["block_date"] = pd.to_datetime(
        prepared["daily_network_activity"]["block_date"], errors="coerce"
    )
    prepared["stablecoin_daily_flows"]["block_date"] = pd.to_datetime(
        prepared["stablecoin_daily_flows"]["block_date"], errors="coerce"
    )
    prepared["stablecoin_network_share"]["block_date"] = pd.to_datetime(
        prepared["stablecoin_network_share"]["block_date"], errors="coerce"
    )

    numeric_columns = {
        "daily_network_activity": [
            "transaction_count",
            "active_senders",
            "active_receivers",
            "successful_transaction_count",
            "total_gas_used",
            "total_gas_paid_eth",
            "avg_base_fee_gwei",
            "block_count",
        ],
        "stablecoin_daily_flows": [
            "transfer_count",
            "transaction_count",
            "active_wallet_count",
            "quantity_transferred",
            "minted_quantity",
            "burned_quantity",
            "net_issuance",
            "mint_event_count",
            "burn_event_count",
        ],
        "stablecoin_network_share": [
            "stablecoin_transfer_count",
            "stablecoin_transaction_count",
            "stablecoin_active_wallet_count",
            "stablecoin_quantity_transferred",
            "network_transaction_count",
            "share_of_network_transactions",
        ],
        "stablecoin_wallet_activity": [
            "transaction_count",
            "gross_inflow",
            "gross_outflow",
            "net_flow",
            "active_days",
        ],
    }

    for frame_name, columns in numeric_columns.items():
        for column in columns:
            if column in prepared[frame_name].columns:
                prepared[frame_name][column] = pd.to_numeric(prepared[frame_name][column], errors="coerce").fillna(0)

    if "is_contract" in prepared["stablecoin_wallet_activity"].columns:
        prepared["stablecoin_wallet_activity"]["is_contract"] = (
            prepared["stablecoin_wallet_activity"]["is_contract"].astype(str).str.lower().isin(["true", "1"])
        )

    return prepared


def format_number(value: float) -> str:
    return f"{value:,.0f}"


def format_decimal(value: float, digits: int = 2) -> str:
    return f"{value:,.{digits}f}"


def format_delta(value: float, digits: int = 2) -> str:
    return f"{value:+,.{digits}f}"


def metric_card(label: str, value: str, delta: str) -> None:
    st.markdown(
        f"""
        <div class="metric-card">
            <div class="metric-label">{label}</div>
            <div class="metric-value">{value}</div>
            <div class="metric-delta">{delta}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def latest_with_prior(df: pd.DataFrame, value_column: str) -> tuple[float, float]:
    ordered = df.sort_values("block_date", ascending=False)
    latest = float(ordered.iloc[0][value_column]) if not ordered.empty else 0.0
    prior = float(ordered.iloc[1:8][value_column].mean()) if len(ordered) > 1 else 0.0
    return latest, prior


def resolve_data_source() -> tuple[dict[str, pd.DataFrame] | None, str | None]:
    st.sidebar.header("Data Source")
    source_mode = st.sidebar.radio(
        "Mode",
        options=["BigQuery", "Artifacts CSV", "Upload CSV"],
        index=0,
        help="BigQuery est recommandé pour un dashboard interactif sur des données fraîches.",
    )

    if source_mode == "BigQuery":
        default_project = os.getenv("GCP_PROJECT_ID", "")
        default_dataset = os.getenv("BIGQUERY_DATASET", "")
        default_location = os.getenv("BIGQUERY_LOCATION", "US")

        with st.sidebar.form("bigquery_connection"):
            project = st.text_input("GCP project", value=default_project)
            dataset = st.text_input("BigQuery dataset", value=default_dataset)
            location = st.text_input("Location", value=default_location)
            submitted = st.form_submit_button("Load from BigQuery", use_container_width=True)

        if submitted:
            if not project or not dataset:
                st.sidebar.error("Renseigne `GCP project` et `BigQuery dataset`.")
            else:
                try:
                    st.session_state["dashboard_frames"] = load_bigquery_frames(project, dataset, location)
                    st.session_state["dashboard_source_label"] = f"BigQuery · {project}.{dataset} ({location})"
                    st.session_state["dashboard_source_mode"] = source_mode
                except Exception as exc:
                    st.sidebar.error(f"BigQuery error: {exc}")

        if st.session_state.get("dashboard_source_mode") == source_mode and "dashboard_frames" in st.session_state:
            return st.session_state["dashboard_frames"], st.session_state.get("dashboard_source_label")

        return None, None

    if source_mode == "Artifacts CSV":
        frames = load_local_frames()
        if frames is None:
            st.sidebar.warning("Les exports ne sont pas encore présents dans `artifacts/`.")
            return None, None
        return frames, "Artifacts CSV · dossier local"

    frames = load_uploaded_frames()
    if frames is None:
        st.sidebar.info("Charge les quatre CSV exportés pour activer le dashboard.")
        return None, None
    return frames, "Upload CSV · session courante"


def render_empty_state() -> None:
    st.markdown(
        """
        <div class="hero">
            <div class="hero-kicker">Interactive Reporting</div>
            <h1 class="hero-title">Stablecoin Intelligence Warehouse</h1>
            <p class="hero-subtitle">
                Le dashboard est prêt, mais aucune source n'est encore connectée.
                Dans la barre latérale, choisis <strong>BigQuery</strong>, renseigne ton projet et ton dataset,
                puis clique sur <strong>Load from BigQuery</strong>. Les modes CSV restent disponibles en secours.
            </p>
            <div class="source-chip">Mode recommandé: BigQuery live</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown("### Ce que le dashboard attend")
    st.markdown(
        """
        - Des tables `dbt` déjà construites dans ton dataset BigQuery
        - Un accès GCP valide via `GOOGLE_APPLICATION_CREDENTIALS`
        - Ou, à défaut, les quatre exports CSV du dossier `artifacts/`
        """
    )

    st.code(
        "\n".join(
            [
                "dbt seed",
                "dbt run --vars '{\"lookback_days\": 30}'",
                "streamlit run streamlit_app.py",
            ]
        ),
        language="bash",
    )


def render_header(latest_day: pd.Timestamp, source_label: str, date_label: str) -> None:
    st.markdown(
        f"""
        <div class="hero">
            <div class="hero-kicker">Onchain BI Reporting</div>
            <h1 class="hero-title">Stablecoin Intelligence Warehouse</h1>
            <p class="hero-subtitle">
                Reporting interactif pour suivre l'activité stablecoin sur Ethereum, comparer USDC, USDT et DAI,
                et identifier rapidement les wallets ou journées qui méritent une revue opérationnelle.
                Dernière date couverte: <strong>{latest_day.date()}</strong>.
            </p>
            <div class="source-chip">{source_label} · {date_label}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def apply_filters(
    frames: dict[str, pd.DataFrame],
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    selected_assets: list[str],
    wallet_segment: str,
) -> tuple[dict[str, pd.DataFrame], bool]:
    network = frames["daily_network_activity"].copy()
    flows = frames["stablecoin_daily_flows"].copy()
    share = frames["stablecoin_network_share"].copy()
    wallets = frames["stablecoin_wallet_activity"].copy()

    network = network[(network["block_date"] >= start_date) & (network["block_date"] <= end_date)]
    flows = flows[(flows["block_date"] >= start_date) & (flows["block_date"] <= end_date)]
    share = share[(share["block_date"] >= start_date) & (share["block_date"] <= end_date)]

    all_assets = sorted(frames["stablecoin_daily_flows"]["asset_symbol"].dropna().unique().tolist())
    selected_assets = selected_assets or all_assets
    flows = flows[flows["asset_symbol"].isin(selected_assets)]
    wallets = wallets[wallets["asset_symbol"].isin(selected_assets)]

    if wallet_segment == "Contracts":
        wallets = wallets[wallets["is_contract"]]
    elif wallet_segment == "EOAs":
        wallets = wallets[~wallets["is_contract"]]

    share_is_estimated = False
    if set(selected_assets) != set(all_assets):
        aggregated_flows = (
            flows.groupby("block_date", as_index=False)
            .agg(
                stablecoin_transfer_count=("transfer_count", "sum"),
                stablecoin_transaction_count=("transaction_count", "sum"),
                stablecoin_active_wallet_count=("active_wallet_count", "sum"),
                stablecoin_quantity_transferred=("quantity_transferred", "sum"),
            )
        )
        share = network[["block_date", "transaction_count"]].merge(aggregated_flows, on="block_date", how="left").fillna(0)
        share = share.rename(columns={"transaction_count": "network_transaction_count"})
        share["share_of_network_transactions"] = share["stablecoin_transaction_count"] / share["network_transaction_count"].replace(0, pd.NA)
        share["share_of_network_transactions"] = share["share_of_network_transactions"].fillna(0)
        share_is_estimated = True

    return {
        "daily_network_activity": network.sort_values("block_date"),
        "stablecoin_daily_flows": flows.sort_values("block_date"),
        "stablecoin_network_share": share.sort_values("block_date"),
        "stablecoin_wallet_activity": wallets,
    }, share_is_estimated


def render_sidebar_filters(frames: dict[str, pd.DataFrame]) -> dict[str, object]:
    st.sidebar.header("Filters")
    min_date = frames["stablecoin_daily_flows"]["block_date"].min().date()
    max_date = frames["stablecoin_daily_flows"]["block_date"].max().date()
    selected_dates = st.sidebar.date_input(
        "Date range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )
    if not isinstance(selected_dates, tuple) or len(selected_dates) != 2:
        selected_dates = (min_date, max_date)

    assets = sorted(frames["stablecoin_daily_flows"]["asset_symbol"].dropna().unique().tolist())
    selected_assets = st.sidebar.multiselect("Assets", options=assets, default=assets)
    wallet_segment = st.sidebar.radio("Wallet segment", options=["All", "EOAs", "Contracts"], index=0)
    top_wallets_limit = st.sidebar.slider("Top wallets", min_value=10, max_value=100, value=25, step=5)
    asset_metric = st.sidebar.selectbox(
        "Asset trend metric",
        options=["quantity_transferred", "net_issuance", "transfer_count"],
        index=0,
    )

    return {
        "start_date": pd.Timestamp(selected_dates[0]),
        "end_date": pd.Timestamp(selected_dates[1]),
        "selected_assets": selected_assets,
        "wallet_segment": wallet_segment,
        "top_wallets_limit": top_wallets_limit,
        "asset_metric": asset_metric,
    }


def render_overview(frames: dict[str, pd.DataFrame], share_is_estimated: bool) -> None:
    network = frames["daily_network_activity"]
    share = frames["stablecoin_network_share"]
    flows = frames["stablecoin_daily_flows"]

    latest_tx, prior_tx = latest_with_prior(network, "transaction_count")
    latest_share, prior_share = latest_with_prior(share, "share_of_network_transactions")
    latest_volume, prior_volume = latest_with_prior(share, "stablecoin_quantity_transferred")
    latest_wallets, prior_wallets = latest_with_prior(share, "stablecoin_active_wallet_count")

    cols = st.columns(4)
    with cols[0]:
        metric_card("Network Transactions", format_number(latest_tx), f"Delta vs 7d avg: {format_delta(latest_tx - prior_tx, 0)}")
    with cols[1]:
        metric_card("Stablecoin Share", format_decimal(latest_share, 4), f"Delta vs 7d avg: {format_delta(latest_share - prior_share, 4)}")
    with cols[2]:
        metric_card("Stablecoin Volume", format_decimal(latest_volume), f"Delta vs 7d avg: {format_delta(latest_volume - prior_volume)}")
    with cols[3]:
        metric_card("Active Wallets", format_number(latest_wallets), f"Delta vs 7d avg: {format_delta(latest_wallets - prior_wallets, 0)}")

    if share_is_estimated:
        st.caption("La part réseau est recalculée sur les assets sélectionnés. Le compteur de wallets agrège les volumes par asset et peut surcompter les wallets présents sur plusieurs stablecoins.")

    left, right = st.columns((1.35, 1))
    with left:
        st.markdown('<div class="section-title">Network and Stablecoin Trend</div>', unsafe_allow_html=True)
        trend = share.merge(
            network[["block_date", "transaction_count", "total_gas_paid_eth"]],
            on="block_date",
            how="left",
        ).sort_values("block_date")
        fig = px.line(
            trend,
            x="block_date",
            y=["transaction_count", "stablecoin_transaction_count"],
            labels={"value": "Transactions", "block_date": "Date", "variable": "Series"},
            color_discrete_sequence=["#13232f", "#0f766e"],
        )
        fig.update_layout(height=360, margin=dict(l=10, r=10, t=10, b=10), legend_title_text="")
        st.plotly_chart(fig, use_container_width=True)

    with right:
        st.markdown('<div class="section-title">Share of Network Transactions</div>', unsafe_allow_html=True)
        fig = px.area(
            share.sort_values("block_date"),
            x="block_date",
            y="share_of_network_transactions",
            color_discrete_sequence=["#0f766e"],
        )
        fig.update_layout(height=360, margin=dict(l=10, r=10, t=10, b=10))
        st.plotly_chart(fig, use_container_width=True)

    st.markdown('<div class="section-title">Stablecoin Volume by Asset</div>', unsafe_allow_html=True)
    volume_fig = px.line(
        flows.sort_values("block_date"),
        x="block_date",
        y="quantity_transferred",
        color="asset_symbol",
        color_discrete_map=PALETTE,
        markers=True,
        labels={"quantity_transferred": "Transferred Quantity", "block_date": "Date", "asset_symbol": "Asset"},
    )
    volume_fig.update_layout(height=420, margin=dict(l=10, r=10, t=10, b=10), legend_title_text="")
    st.plotly_chart(volume_fig, use_container_width=True)


def render_assets(frames: dict[str, pd.DataFrame], asset_metric: str) -> None:
    flows = frames["stablecoin_daily_flows"].sort_values("block_date")
    assets = sorted(flows["asset_symbol"].dropna().unique().tolist())
    if not assets:
        st.info("Aucun asset disponible avec les filtres courants.")
        return

    selected_asset = st.selectbox("Stablecoin", options=assets, index=0)
    asset_df = flows[flows["asset_symbol"] == selected_asset].copy().sort_values("block_date")
    latest = asset_df.sort_values("block_date", ascending=False).iloc[0]
    prior_window = asset_df.sort_values("block_date", ascending=False).iloc[1:8]

    cols = st.columns(4)
    with cols[0]:
        metric_card("Transferred", format_decimal(float(latest["quantity_transferred"])), f"Issuer: {latest['issuer']}")
    with cols[1]:
        metric_card("Net Issuance", format_decimal(float(latest["net_issuance"])), f"Minted: {format_decimal(float(latest['minted_quantity']))}")
    with cols[2]:
        metric_card(
            "Active Wallets",
            format_number(float(latest["active_wallet_count"])),
            f"7d avg: {format_decimal(float(prior_window['active_wallet_count'].mean()) if not prior_window.empty else 0, 0)}",
        )
    with cols[3]:
        metric_card("Transfer Count", format_number(float(latest["transfer_count"])), f"Burned: {format_decimal(float(latest['burned_quantity']))}")

    left, right = st.columns(2)
    with left:
        st.markdown('<div class="section-title">Trend by Selected Metric</div>', unsafe_allow_html=True)
        chart_type = st.radio("Chart", options=["Bar", "Line"], horizontal=True, key="asset_chart_type")
        if chart_type == "Bar":
            fig = px.bar(
                asset_df,
                x="block_date",
                y=asset_metric,
                color_discrete_sequence=[PALETTE.get(selected_asset, "#0f766e")],
            )
        else:
            fig = px.line(
                asset_df,
                x="block_date",
                y=asset_metric,
                markers=True,
                color_discrete_sequence=[PALETTE.get(selected_asset, "#0f766e")],
            )
        fig.update_layout(height=360, margin=dict(l=10, r=10, t=10, b=10), yaxis_title=asset_metric)
        st.plotly_chart(fig, use_container_width=True)

    with right:
        st.markdown('<div class="section-title">Mint vs Burn vs Net</div>', unsafe_allow_html=True)
        issuance = asset_df.melt(
            id_vars="block_date",
            value_vars=["minted_quantity", "burned_quantity", "net_issuance"],
            var_name="metric",
            value_name="amount",
        )
        fig = px.line(
            issuance,
            x="block_date",
            y="amount",
            color="metric",
            color_discrete_sequence=["#0f766e", "#c2410c", "#13232f"],
        )
        fig.update_layout(height=360, margin=dict(l=10, r=10, t=10, b=10), legend_title_text="")
        st.plotly_chart(fig, use_container_width=True)

    st.markdown('<div class="section-title">Latest Daily Rows</div>', unsafe_allow_html=True)
    st.dataframe(asset_df.sort_values("block_date", ascending=False).head(20), use_container_width=True, hide_index=True)


def render_wallets(frames: dict[str, pd.DataFrame], top_wallets_limit: int) -> None:
    wallets = frames["stablecoin_wallet_activity"].copy()
    if wallets.empty:
        st.info("Aucun wallet disponible avec les filtres courants.")
        return

    ranking_mode = st.radio(
        "Ranking mode",
        options=["Absolute net flow", "Gross inflow", "Gross outflow", "Transaction count"],
        horizontal=True,
    )
    sort_column = {
        "Absolute net flow": "abs_net_flow",
        "Gross inflow": "gross_inflow",
        "Gross outflow": "gross_outflow",
        "Transaction count": "transaction_count",
    }[ranking_mode]

    wallets["abs_net_flow"] = wallets["net_flow"].abs()
    top_wallets = wallets.sort_values(sort_column, ascending=False).head(top_wallets_limit)

    left, right = st.columns((1.15, 0.85))
    with left:
        st.markdown('<div class="section-title">Top Wallets</div>', unsafe_allow_html=True)
        st.dataframe(
            top_wallets[
                [
                    "asset_symbol",
                    "issuer",
                    "wallet_address",
                    "transaction_count",
                    "gross_inflow",
                    "gross_outflow",
                    "net_flow",
                    "active_days",
                    "is_contract",
                ]
            ],
            use_container_width=True,
            hide_index=True,
        )

    with right:
        st.markdown('<div class="section-title">Wallet Type Mix</div>', unsafe_allow_html=True)
        contract_mix = (
            wallets.groupby("is_contract", dropna=False)["wallet_address"]
            .nunique()
            .reset_index(name="wallet_count")
            .replace({"is_contract": {True: "Contracts", False: "EOAs"}})
        )
        fig = px.pie(contract_mix, names="is_contract", values="wallet_count", color_discrete_sequence=["#13232f", "#0f766e"])
        fig.update_layout(height=360, margin=dict(l=10, r=10, t=10, b=10))
        st.plotly_chart(fig, use_container_width=True)


def render_ops_brief(frames: dict[str, pd.DataFrame]) -> None:
    summary = build_summary(
        frames["daily_network_activity"].sort_values("block_date", ascending=False).to_dict(orient="records"),
        frames["stablecoin_daily_flows"].sort_values(["block_date", "asset_symbol"], ascending=[False, True]).to_dict(orient="records"),
        frames["stablecoin_network_share"].sort_values("block_date", ascending=False).to_dict(orient="records"),
        frames["stablecoin_wallet_activity"].sort_values("net_flow", ascending=False).to_dict(orient="records"),
    )
    markdown = build_markdown(summary)

    left, right = st.columns((1.15, 0.85))
    with left:
        st.markdown('<div class="section-title">Markdown Brief</div>', unsafe_allow_html=True)
        st.markdown(markdown)
        st.download_button(
            "Download brief",
            data=markdown,
            file_name="stablecoin_ops_brief.md",
            mime="text/markdown",
            use_container_width=True,
        )

    with right:
        st.markdown('<div class="section-title">LLM Context</div>', unsafe_allow_html=True)
        st.json(summary)
        st.download_button(
            "Download JSON context",
            data=json.dumps(summary, indent=2),
            file_name="llm_context.json",
            mime="application/json",
            use_container_width=True,
        )


def main() -> None:
    inject_css()

    raw_frames, source_label = resolve_data_source()
    if raw_frames is None:
        render_empty_state()
        return

    frames = prepare_dataframes(raw_frames)
    if any(frame.empty for frame in frames.values()):
        st.warning("La source a répondu, mais au moins un dataset est vide. Vérifie que les tables dbt sont bien construites dans BigQuery.")
        return

    filters = render_sidebar_filters(frames)
    filtered_frames, share_is_estimated = apply_filters(
        frames,
        filters["start_date"],
        filters["end_date"],
        filters["selected_assets"],
        filters["wallet_segment"],
    )

    if filtered_frames["stablecoin_daily_flows"].empty or filtered_frames["stablecoin_network_share"].empty:
        st.warning("Les filtres actuels ne retournent aucune donnée.")
        return

    latest_day = filtered_frames["stablecoin_network_share"]["block_date"].max()
    date_label = f"Scope: {filters['start_date'].date()} -> {filters['end_date'].date()}"
    render_header(latest_day, source_label or "Data source", date_label)

    overview_tab, assets_tab, wallets_tab, brief_tab = st.tabs(["Overview", "Assets", "Wallets", "Ops Brief"])

    with overview_tab:
        render_overview(filtered_frames, share_is_estimated)

    with assets_tab:
        render_assets(filtered_frames, filters["asset_metric"])

    with wallets_tab:
        render_wallets(filtered_frames, filters["top_wallets_limit"])

    with brief_tab:
        render_ops_brief(filtered_frames)


if __name__ == "__main__":
    main()
