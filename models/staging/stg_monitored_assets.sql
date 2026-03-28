{{ config(materialized='view') }}

select
  lower(contract_address) as contract_address,
  asset_name,
  upper(asset_symbol) as asset_symbol,
  lower(asset_type) as asset_type,
  lower(chain) as chain,
  issuer,
  cast(decimals as int64) as decimals
from {{ ref('monitored_assets') }}
