select route_id, avg(loss), avg(avg_latency), avg(stddev_latency), count(*) from mtr group by route_id;
