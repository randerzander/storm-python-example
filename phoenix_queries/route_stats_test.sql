upsert into mtr.mtr values ('192.168.1.2','192.168.1.1', 1,0,23.4, .4);
upsert into mtr.mtr values ('192.168.1.2','192.168.1.1', 2, 0, 25, .5);
upsert into mtr.mtr values ('192.168.1.2','192.168.1.1', 3, 0, 24, .2);

select route_id, avg(loss), avg(avg_latency), avg(stddev_latency), count(*) from mtr group by route_id;
