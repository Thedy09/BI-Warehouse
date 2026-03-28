{{ config(materialized='view') }}

select
  cast(block_number as int64) as block_number,
  transaction_hash,
  lower(from_address) as from_address,
  lower(to_address) as to_address,
  timestamp(block_timestamp) as block_timestamp,
  date(block_timestamp) as block_date,
  safe_cast(value as bignumeric) as value_wei
from {{ source('ethereum', 'transactions') }}
where date(block_timestamp) >= date_sub(current_date('UTC'), interval {{ var('lookback_days', 30) }} day)

