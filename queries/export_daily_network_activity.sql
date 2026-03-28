select *
from `{{ target_project }}.{{ target_dataset }}.fct_daily_network_activity`
order by block_date desc
