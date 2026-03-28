{{ config(materialized='view') }}

select
  lower(address) as address,
  cast(is_contract as bool) as is_contract
from {{ source('ethereum', 'accounts') }}

