select distinct auth_company.title, 1 as cooked 
from cook_task
inner join auth_user on cook_task.user_id = auth_user.id
inner join auth_company on auth_company.id = auth_user.company_id 
where
date(FROM_UNIXTIME(`sg-prod-fusion`.cook_task.finish_time/1000)) between '2016-11-26' and '2016-12-01';