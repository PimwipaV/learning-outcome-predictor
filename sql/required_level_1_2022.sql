select distinct cl.id as cri_levels_id, cl.cri_id, cl.level, cl.flag , a.id as activity_id
from cri_levels cl
join act_sub_points asp on cl.cri_id = asp.cri_id and cl.deleted_at is null
join activities a on asp.activity_id = a.id
where cl.cri_id in (2922251, 3112276, 3199245, 3125413, 3118906, 3198357, 3199253,
       3199266, 2638174, 2638332, 2638345, 3152158, 3131914) and flag = 1
order by cl.cri_id;