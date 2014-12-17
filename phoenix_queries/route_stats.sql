select 
  source_ip || '|' || target_ip as route_id,
  avg(loss),
  avg(avg_latency),
  avg(stddev_latency)
from mtr.mtr
group by route_id
limit 10;
