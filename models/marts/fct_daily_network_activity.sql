{{ config(materialized='table') }}

with tx_base as (
  select
    t.block_date,
    t.transaction_hash,
    t.from_address,
    t.to_address,
    r.tx_status,
    r.gas_used,
    r.effective_gas_price_wei
  from {{ ref('stg_eth_transactions') }} as t
  left join {{ ref('stg_eth_receipts') }} as r
    on t.block_number = r.block_number
   and t.transaction_hash = r.transaction_hash
),
tx_daily as (
  select
    block_date,
    count(*) as transaction_count,
    count(distinct from_address) as active_senders,
    count(distinct to_address) as active_receivers,
    countif(tx_status = 1) as successful_transaction_count,
    sum(gas_used) as total_gas_used,
    sum((effective_gas_price_wei * gas_used) / cast(1000000000000000000 as bignumeric)) as total_gas_paid_eth
  from tx_base
  group by 1
),
block_daily as (
  select
    block_date,
    count(*) as block_count,
    avg(base_fee_per_gas_wei / cast(1000000000 as bignumeric)) as avg_base_fee_gwei
  from {{ ref('stg_eth_blocks') }}
  group by 1
)
select
  tx_daily.block_date,
  block_daily.block_count,
  tx_daily.transaction_count,
  tx_daily.active_senders,
  tx_daily.active_receivers,
  tx_daily.successful_transaction_count,
  tx_daily.total_gas_used,
  tx_daily.total_gas_paid_eth,
  block_daily.avg_base_fee_gwei
from tx_daily
left join block_daily
  using (block_date)

