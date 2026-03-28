{{ config(materialized='table') }}

with stablecoin_transfers as (
  select
    t.block_date,
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
wallets_by_day as (
  select
    block_date,
    count(distinct wallet_address) as stablecoin_active_wallet_count
  from (
    select block_date, from_address as wallet_address from stablecoin_transfers
    union distinct
    select block_date, to_address as wallet_address from stablecoin_transfers
  )
  where wallet_address != '0x0000000000000000000000000000000000000000'
  group by 1
),
stablecoin_daily as (
  select
    t.block_date,
    count(*) as stablecoin_transfer_count,
    count(distinct t.transaction_hash) as stablecoin_transaction_count,
    w.stablecoin_active_wallet_count,
    sum(t.quantity_token) as stablecoin_quantity_transferred
  from stablecoin_transfers as t
  left join wallets_by_day as w
    using (block_date)
  group by 1, 4
)
select
  s.block_date,
  s.stablecoin_transfer_count,
  s.stablecoin_transaction_count,
  s.stablecoin_active_wallet_count,
  s.stablecoin_quantity_transferred,
  n.transaction_count as network_transaction_count,
  safe_divide(s.stablecoin_transaction_count, n.transaction_count) as share_of_network_transactions
from stablecoin_daily as s
left join {{ ref('fct_daily_network_activity') }} as n
  using (block_date)

