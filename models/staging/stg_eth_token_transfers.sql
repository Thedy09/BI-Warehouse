{{ config(materialized='view') }}

select
  transaction_hash,
  timestamp(block_timestamp) as block_timestamp,
  date(block_timestamp) as block_date,
  lower(address) as token_address,
  lower(from_address) as from_address,
  lower(to_address) as to_address,
  safe_cast(quantity as bignumeric) as quantity_raw
from {{ source('ethereum', 'token_transfers') }}
where date(block_timestamp) >= date_sub(current_date('UTC'), interval {{ var('lookback_days', 30) }} day)

