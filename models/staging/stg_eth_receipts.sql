{{ config(materialized='view') }}

select
  cast(block_number as int64) as block_number,
  transaction_hash,
  safe_cast(gas_used as bignumeric) as gas_used,
  safe_cast(effective_gas_price as bignumeric) as effective_gas_price_wei,
  cast(status as int64) as tx_status
from {{ source('ethereum', 'receipts') }}

