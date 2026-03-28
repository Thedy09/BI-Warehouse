select *
from `{{ target_project }}.{{ target_dataset }}.fct_stablecoin_daily_flows`
order by block_date desc, asset_symbol

