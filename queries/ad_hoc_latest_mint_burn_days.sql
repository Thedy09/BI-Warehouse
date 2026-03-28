select
  block_date,
  asset_symbol,
  issuer,
  minted_quantity,
  burned_quantity,
  net_issuance,
  mint_event_count,
  burn_event_count
from `{{ target_project }}.{{ target_dataset }}.fct_stablecoin_daily_flows`
where mint_event_count > 0 or burn_event_count > 0
order by block_date desc, abs(net_issuance) desc
limit 100

