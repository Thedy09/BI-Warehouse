with usdc_asset as (
  select contract_address
  from `{{ target_project }}.{{ target_dataset }}.stg_monitored_assets`
  where asset_symbol = 'USDC'
),
transfers as (
  select
    lower(t.address) as token,
    lower(t.to_address) as account,
    cast(0 as bignumeric) as amount_out,
    safe_cast(t.quantity as bignumeric) as amount_in
  from `bigquery-public-data.goog_blockchain_ethereum_mainnet_us.token_transfers` as t

  union all

  select
    lower(t.address) as token,
    lower(t.from_address) as account,
    safe_cast(t.quantity as bignumeric) as amount_out,
    cast(0 as bignumeric) as amount_in
  from `bigquery-public-data.goog_blockchain_ethereum_mainnet_us.token_transfers` as t
)
select
  account,
  safe_divide(sum(amount_in) - sum(amount_out), cast(1000000 as bignumeric)) as usdc_balance
from transfers
where token in (select contract_address from usdc_asset)
group by 1
order by usdc_balance desc
limit 20
