select distinct cl.id as cri_levels_id, cl.cri_id, cl.level, cl.flag , a.id as activity_id
from cri_levels cl
join act_sub_points asp on cl.cri_id = asp.cri_id and cl.deleted_at is null
join activities a on asp.activity_id = a.id
where cl.cri_id in (%s) and flag = 1
order by cl.cri_id;