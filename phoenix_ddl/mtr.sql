drop table if exists mtr.mtr;

create table mtr.mtr(
  source_ip varchar not null,
  target_ip varchar not null,
  local_time integer not null,
  loss double,
  avg_latency double,
  stddev_latency double 
  constraint my_pk primary key (source_ip, target_ip, local_time)
) ttl=432000
;
