{{ config(materialized='table') }}

with stablecoin_transfers as (
  select
    a.asset_name,
    a.asset_symbol,
    a.issuer,
    t.block_timestamp,
    t.transaction_hash,
    t.from_address,
    t.to_address,
    safe_divide(
      t.quantity_raw,
      cast(concat('1', repeat('0', a.decimals)) as bignumeric)
    ) as quantity_token
  from {{ ref('stg_eth_token_transfers') }} as t
  inner join {{ ref('stg_monitored_assets') }} as a
    on t.token_address = a.contract_address
),
wallet_events as (
  select
    asset_name,
    asset_symbol,
    issuer,
    from_address as wallet_address,
    block_timestamp,
    transaction_hash,
    quantity_token as gross_outflow,
    cast(0 as bignumeric) as gross_inflow
  from stablecoin_transfers

  union all

  select
    asset_name,
    asset_symbol,
    issuer,
    to_address as wallet_address,
    block_timestamp,
    transaction_hash,
    cast(0 as bignumeric) as gross_outflow,
    quantity_token as gross_inflow
  from stablecoin_transfers
)
select
  e.asset_name,
  e.asset_symbol,
  e.issuer,
  e.wallet_address,
  min(e.block_timestamp) as first_seen_at,
  max(e.block_timestamp) as last_seen_at,
  count(distinct e.transaction_hash) as transaction_count,
  sum(e.gross_inflow) as gross_inflow,
  sum(e.gross_outflow) as gross_outflow,
  sum(e.gross_inflow) - sum(e.gross_outflow) as net_flow,
  count(distinct date(e.block_timestamp)) as active_days,
  coalesce(a.is_contract, false) as is_contract
from wallet_events as e
left join {{ ref('stg_eth_accounts') }} as a
  on e.wallet_address = a.address
where e.wallet_address is not null
  and e.wallet_address != '0x0000000000000000000000000000000000000000'
group by 1, 2, 3, 4, 12

