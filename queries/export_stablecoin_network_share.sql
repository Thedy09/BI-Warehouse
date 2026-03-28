select *
from `{{ target_project }}.{{ target_dataset }}.fct_stablecoin_network_share`
order by block_date desc

