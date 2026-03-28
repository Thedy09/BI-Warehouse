{{ config(materialized='view') }}

select
  cast(block_number as int64) as block_number,
  timestamp(block_timestamp) as block_timestamp,
  date(block_timestamp) as block_date,
  safe_cast(base_fee_per_gas as bignumeric) as base_fee_per_gas_wei
from {{ source('ethereum', 'blocks') }}
where date(block_timestamp) >= date_sub(current_date('UTC'), interval {{ var('lookback_days', 30) }} day)

