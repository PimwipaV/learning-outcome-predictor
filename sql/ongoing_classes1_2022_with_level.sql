#จะเอา class ที่เป็น OBEM ของ semesterัปัจจุบันที่ดำเนินการเรียนการสอนอยู่เท่านั้น 2/2022
SELECT DISTINCT
    co.course_id,
    co.clo_id,
    co.class_id,
    asp.user_id, 
    asp.activity_id,
    asp.cri_id,
    s.slug,
    a.type,
    a.start_date,
    asp.cri_id,
    asub.id AS submission_id,
    asub.submitted_at,
    asub.spent_time,
    a.due_date,
    cl.level

from course_outcomes co join classes c on c.id = co.class_id
join activities a on a.class_id = c.id
inner join semesters s on s.id = c.semester_id
join act_sub_points asp on asp.activity_id = a.id 
join criterias cri on cri.id = asp.cri_id and cri.clo_id = co.id and cri.deleted_at is null
join activity_submissions asub on asub.activity_id = a.id and asp.user_id = asub.user_id 
join cri_levels cl on cl.id = asp.cri_level_id and cl.deleted_at is null 

where c.type = "obem" and co.type = "ulti" and asub.deleted_at is null 
and (s.slug = '1/2022')
and c.deleted_at is null
and co.deleted_at is null
and c.code in (
	select cla.code
	from activities act
	inner join classes cla on cla.id = act.class_id
	where cla.deleted_at is null
		and act.deleted_at is null
		and cla.semester_id = c.semester_id
		and cla.type = c.type
	group by cla.id
    ) 
order by co.class_id, asp.activity_id, s.slug, asub.submitted_at;