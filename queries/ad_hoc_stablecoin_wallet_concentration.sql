select
  asset_symbol,
  wallet_address,
  issuer,
  gross_inflow,
  gross_outflow,
  net_flow,
  transaction_count,
  is_contract
from `{{ target_project }}.{{ target_dataset }}.dim_stablecoin_wallet_activity`
order by abs(net_flow) desc
limit 50

