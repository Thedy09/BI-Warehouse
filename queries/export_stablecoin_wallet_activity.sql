select *
from `{{ target_project }}.{{ target_dataset }}.dim_stablecoin_wallet_activity`
order by abs(net_flow) desc, transaction_count desc
limit 100

