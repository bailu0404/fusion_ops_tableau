select distinct auth_company.title as company, date(FROM_UNIXTIME(`sg-prod-fusion`.cook_task.finish_time/1000)) as active_date
from cook_task
inner join auth_user on cook_task.user_id = auth_user.id
inner join auth_company on auth_company.id = auth_user.company_id
order by active_date;