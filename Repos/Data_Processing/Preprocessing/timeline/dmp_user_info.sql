set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
drop table if exists marketing_modeling.mm_dmp_user_info;
create table marketing_modeling.mm_dmp_user_info as 
select b.*,
ROW_NUMBER() OVER(PARTITION BY b.phone ORDER BY b.reqtime desc) AS rn
from (
SELECT 
a.cookie.phonenumber as phone,
a.tinfo.creative, 
a.tinfo.media,
from_unixtime(cast(a.reqtime/1000 as bigint)) as reqtime
FROM mipsaic.edw_dmp_detailbase a
where a.cookie.phonenumber is not null
) b
