{{ config(materialized='table') }}

with stablecoin_transfers as (
  select
    t.block_date,
    t.block_timestamp,
    t.transaction_hash,
    a.asset_name,
    a.asset_symbol,
    a.issuer,
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
wallets_by_asset_day as (
  select
    block_date,
    asset_symbol,
    count(distinct wallet_address) as active_wallet_count
  from (
    select block_date, asset_symbol, from_address as wallet_address from stablecoin_transfers
    union distinct
    select block_date, asset_symbol, to_address as wallet_address from stablecoin_transfers
  )
  where wallet_address != '0x0000000000000000000000000000000000000000'
  group by 1, 2
)
select
  t.block_date,
  t.asset_name,
  t.asset_symbol,
  t.issuer,
  count(*) as transfer_count,
  count(distinct t.transaction_hash) as transaction_count,
  w.active_wallet_count,
  sum(t.quantity_token) as quantity_transferred,
  sum(case when t.from_address = '0x0000000000000000000000000000000000000000' then t.quantity_token else 0 end) as minted_quantity,
  sum(case when t.to_address = '0x0000000000000000000000000000000000000000' then t.quantity_token else 0 end) as burned_quantity,
  sum(case when t.from_address = '0x0000000000000000000000000000000000000000' then t.quantity_token else 0 end)
    - sum(case when t.to_address = '0x0000000000000000000000000000000000000000' then t.quantity_token else 0 end) as net_issuance,
  countif(t.from_address = '0x0000000000000000000000000000000000000000') as mint_event_count,
  countif(t.to_address = '0x0000000000000000000000000000000000000000') as burn_event_count
from stablecoin_transfers as t
left join wallets_by_asset_day as w
  on t.block_date = w.block_date
 and t.asset_symbol = w.asset_symbol
group by 1, 2, 3, 4, 7

