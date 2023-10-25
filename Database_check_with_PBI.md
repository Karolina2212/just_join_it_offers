# In order to verify data between database and visualisation below query is used:

## CREATED VIEW

```sql
CREATE OR REPLACE VIEW public.vw_all_data
AS SELECT oi.id,
    oi.jjit_id,
    oi.title,
    oi.company_name,
    oi.marker_icon,
        CASE
            WHEN oi.workplace_type = 'partly_remote'::text THEN 'hybrid'::text
            ELSE oi.workplace_type
        END AS workplace_type,
    oi.experience_level,
    oi.import_date,
    oet.empl_type,
    oet.salary_from,
    oet.salary_to,
    oet.currency,
    er.rate,
    ol.city,
    ol.country,
    os.skill_name,
        CASE
            WHEN er.rate IS NOT NULL THEN oet.salary_from * er.rate
            ELSE oet.salary_from
        END AS salary_from_pln,
        CASE
            WHEN er.rate IS NOT NULL THEN oet.salary_to * er.rate
            ELSE oet.salary_to
        END AS salary_to_pln,
      case 
      	when salary_from = 0 or salary_to = 0 then 'not disclosed'
      	else 'disclosed'
      end as salary_disclosure 
   FROM offers_info oi
     JOIN offers_empl_type oet ON oi.id = oet.offer_id
     LEFT JOIN exchange_rates er ON er.currency_code = oet.currency
     JOIN offers_per_location_id opli ON oi.id = opli.offer_id
     JOIN offers_locations ol ON opli.location_id = ol.id
     JOIN skills_per_offer spo ON oi.id = spo.offer_id
     JOIN offers_skills os ON os.id = spo.skill_id;
```
## CREATED FUNCTIONS:

### total number of unique offers

```sql
CREATE OR REPLACE FUNCTION public.total_num_of_offers(integer)
 RETURNS numeric
 LANGUAGE plpgsql
AS $function$
begin 
	return(select count(distinct jjit_id) from vw_all_data);
end;
$function$
;
```

### number of unique offers per selected import date
```sql
CREATE OR REPLACE FUNCTION public.num_of_offers_previous_import(last_import date, specjalization text)
 RETURNS numeric
 LANGUAGE plpgsql
AS $function$
begin 
	return(select count(distinct jjit_id) from vw_all_data vw where vw.import_date = last_import and vw.marker_icon = specjalization);
end;
$function$
;
```

### number of unique offers per selected attributes (col_name and filters)
```sql
CREATE OR REPLACE FUNCTION public.attribute_share(column_name text, specjalization text, last_import date, experience_level text)
 RETURNS TABLE(attribute text, offer_count bigint)
 LANGUAGE plpgsql
AS $function$
declare sql_query text;
begin
	
	if specjalization is not null and last_import is not null and experience_level is not null then 
	
	sql_query := format('SELECT DISTINCT %I, dense_rank() over (partition by %I order by jjit_id) + dense_rank() over (partition by %I order by jjit_id desc) - 1 as offer_count from vw_all_data where marker_icon = $1 and import_date = $2 and experience_level = $3', column_name,column_name,column_name);
    RETURN QUERY EXECUTE sql_query using specjalization, last_import, experience_level ;
   
	
	elseif specjalization is not null and last_import is not null then 
	
	sql_query := format('SELECT DISTINCT %I, dense_rank() over (partition by %I order by jjit_id) + dense_rank() over (partition by %I order by jjit_id desc) - 1 as offer_count from vw_all_data where marker_icon = $1 and import_date = $2', column_name,column_name,column_name);
    RETURN QUERY EXECUTE sql_query using specjalization, last_import ;
   
   elseif specjalization is not null and experience_level is not null then 
	
	sql_query := format('SELECT DISTINCT %I, dense_rank() over (partition by %I order by jjit_id) + dense_rank() over (partition by %I order by jjit_id desc) - 1 as offer_count from vw_all_data where marker_icon = $1 and experience_level = $2', column_name,column_name,column_name);
    RETURN QUERY EXECUTE sql_query using specjalization, experience_level ;
   
   elseif specjalization is not null then
   
   sql_query := format('SELECT DISTINCT %I, dense_rank() over (partition by %I order by jjit_id) + dense_rank() over (partition by %I order by jjit_id desc) - 1 as offer_count from vw_all_data where marker_icon = $1', column_name,column_name,column_name);
    RETURN QUERY EXECUTE sql_query using specjalization;
   
   else
   
   sql_query := format('SELECT DISTINCT %I, dense_rank() over (partition by %I order by jjit_id) + dense_rank() over (partition by %I order by jjit_id desc) - 1 as offer_count from vw_all_data ', column_name,column_name,column_name);
    RETURN QUERY EXECUTE sql_query;
   
   end if;
   
end;
$function$
;

```
### salary disclosure share
```sql
CREATE OR REPLACE FUNCTION public.disclosure_share(column_name text, specjalization text, employment_types text[], disclosure text)
 RETURNS TABLE(attribute text, disc_share numeric)
 LANGUAGE plpgsql
AS $function$
declare sql_query text;
begin
	
if specjalization is not null then

sql_query := format('
with disclosure as (
	SELECT DISTINCT 
		%I, 
		salary_disclosure, 
		dense_rank() over (partition by %I,salary_disclosure order by jjit_id) + dense_rank() over (partition by %I,salary_disclosure order by jjit_id desc) - 1 as offer_count 
	from vw_all_data 
	where empl_type in (SELECT unnest($1)) and marker_icon = $2 order by %I)
select %I, round(offer_count*100/sum(offer_count) over (partition by %I),0) as disc_share from disclosure', column_name,column_name,column_name,column_name,column_name,column_name);
    RETURN QUERY EXECUTE sql_query using employment_types, specjalization;
    
 else
 
 sql_query := format('
with disclosure as (
	SELECT DISTINCT 
		%I, 
		salary_disclosure, 
		dense_rank() over (partition by %I,salary_disclosure order by jjit_id) + dense_rank() over (partition by %I,salary_disclosure order by jjit_id desc) - 1 as offer_count 
	from vw_all_data 
	where empl_type in (SELECT unnest($1)) order by %I )
select %I, round(offer_count*100/sum(offer_count) over (partition by %I),0) as disc_share from disclosure where disclosure.salary_disclosure = $2', column_name,column_name,column_name,column_name,column_name,column_name);
    RETURN QUERY EXECUTE sql_query using employment_types, disclosure;

 end if;
 end;
$function$
;
```

### calculation of unified salary ranges for each offer (used to calculate median)
```sql
CREATE OR REPLACE FUNCTION public.salary_uni_ranges(employment_types text[], specjalization text DEFAULT NULL::text, median_total text DEFAULT NULL::text)
 RETURNS TABLE(jjit_id text, experience_level text, empl_type text, offer_sal_from numeric, offer_sal_to numeric, marker_icon text)
 LANGUAGE plpgsql
AS $function$
begin
	if median_total = 'median_total' then
		return query
		select distinct
				w1.jjit_id,
				w1.experience_level,
				w1.empl_type,
				avg(w1.salary_from_pln) over (partition by w1.jjit_id, w1.experience_level, w1.empl_type) as offer_sal_from,
				avg(w1.salary_to_pln) over (partition by w1.jjit_id, w1.experience_level, w1.empl_type) as offer_sal_to,
				NULL
			from vw_all_data w1 
			where 
				w1.salary_disclosure = 'disclosed' 
				and 
				w1.empl_type in (SELECT unnest(employment_types));
	elseif specjalization is not null then
		return query
		select distinct
				w1.jjit_id,
				w1.experience_level,
				w1.empl_type,
				avg(w1.salary_from_pln) over (partition by w1.jjit_id, w1.experience_level, w1.empl_type, w1.marker_icon) as offer_sal_from,
				avg(w1.salary_to_pln) over (partition by w1.jjit_id, w1.experience_level, w1.empl_type, w1.marker_icon) as offer_sal_to,
				w1.marker_icon
			from vw_all_data w1 
			where 
				w1.salary_disclosure = 'disclosed' 
				and 
				w1.empl_type in (SELECT unnest(employment_types))
				and
				w1.marker_icon = specjalization;
	else	
		return query
		select distinct
				w1.jjit_id,
				w1.experience_level,
				w1.empl_type,
				avg(w1.salary_from_pln) over (partition by w1.jjit_id, w1.experience_level, w1.empl_type, w1.marker_icon) as offer_sal_from,
				avg(w1.salary_to_pln) over (partition by w1.jjit_id, w1.experience_level, w1.empl_type, w1.marker_icon) as offer_sal_to,
				w1.marker_icon
			from vw_all_data w1 
			where 
				w1.salary_disclosure = 'disclosed' 
				and 
				w1.empl_type in (SELECT unnest(employment_types));
	end if;
end;
$function$
;
```

## CHECK PER PBI PAGES

### OFFERS OVERVIEW

#### total num of unique offers
```sql
select * from total_num_of_offers(1) 
```

#### workplace_type share 
```sql
with workplace_num_of_offers as (select * from public.attribute_share('workplace_type',null,null,null))
select 
	wo.attribute, 
	round(wo.offer_count*100/(select sum(wo2.offer_count) from workplace_num_of_offers wo2),1) as share_in_total 
from workplace_num_of_offers wo;
```

#### experience_level share
```sql
with seniority_num_of_offers as (select * from public.attribute_share('experience_level',null,null,null))
select distinct
	so.attribute, 
	round(so.offer_count*100/(select sum(so2.offer_count) from seniority_num_of_offers so2),1) as share_in_total 
from seniority_num_of_offers so;
```

##### specjalization share
```sql
with 
	specjalization_num_of_offers as (select * from public.attribute_share('marker_icon',null,null,null)), 
	offers_num as (select total_num_of_offers from total_num_of_offers(1))
select 
	spo.attribute, 
	round(spo.offer_count*100/(select total_num_of_offers from offers_num) ,1) as share_in_total 
from specjalization_num_of_offers spo order by share_in_total desc;
```

##### specjalization last import week insight
```sql
with 
	num_last_import as (
	    select 
	        num_of_offers_previous_import as off_num_li 
	    from public.num_of_offers_previous_import('2023-10-18','data')),
	num_prev_import as (
	    select 
	        num_of_offers_previous_import as off_num_pi 
	    from public.num_of_offers_previous_import('2023-10-12','data'))
select 
    ROUND((SUM(li.off_num_li) - SUM(pi.off_num_pi))*100/SUM(pi.off_num_pi),1) 
from num_last_import li 
full JOIN num_prev_import pi 
    on li.off_num_li = pi.off_num_pi

with seniority_num_of_offers_spec as(
    select * from public.attribute_share('experience_level','data','2023-10-18',null)
    )
select distinct
	sso.attribute, 
	round(sso.offer_count*100/(select sum(sso2.offer_count) from seniority_num_of_offers_spec sso2),0) as share_in_total 
from seniority_num_of_offers_spec sso;

with workplace_num_of_offers_spec as (
    select * from public.attribute_share('workplace_type','data','2023-10-18',null))
select distinct
	swo.attribute, 
	round(swo.offer_count*100/(select sum(swo2.offer_count) from workplace_num_of_offers_spec swo2),1) as share_in_total 
from workplace_num_of_offers_spec swo;
```

### SALARY OVERVIEW

#### offers per employment_type (with % of salary disclosed)
```sql
with disclosure as (
	select * from public.disclosure_share('empl_type',null,array['b2b','permanent','contract','mandate_contract'])
	)
select 
	disclosure.attribute, 
	ems.offer_count as active_offers, 
	disclosure.disc_share as offers_disclosed_share 
from public.attribute_share('empl_type',null,null,null) ems
left join disclosure
on ems.attribute = disclosure.attribute
where ems.attribute not in ('internship', 'any') and disclosure.salary_disclosure = 'disclosed'
order by ems.offer_count desc;
```

#### median salary per empl_type and experience level
```sql
with med_total as(
	select 
		ofsal.*,
		(ofsal.offer_sal_from + ofsal.offer_sal_to)/2 as median_offer 
		from (select * from public.salary_uni_ranges(array['b2b','permanent','contract','mandate_contract'],null,'median_total')) ofsal)
select 
	med_total.experience_level,
	med_total.empl_type,
	percentile_cont(0.5) within group (order by med_total.median_offer)
from med_total group by med_total.experience_level, med_total.empl_type order by med_total.experience_level;
```

#### top best paid specialization
```sql
with sal_med as(
	select 
		ofsal.*, 
		(ofsal.offer_sal_from + ofsal.offer_sal_to)/2 as median_offer 
		from (select * from public.salary_uni_ranges(array['permanent'])) ofsal)
select 
	sal_med.marker_icon,
	percentile_cont(0.5) within group (order by sal_med.median_offer) as median
from sal_med group by sal_med.marker_icon order by median desc limit 4;
```

### SALARY PER SPECJALIZATION

#### median salary per empl_type and specialization
```sql
select 
	ofsal.experience_level,
	percentile_cont(0.5) within group (order by ofsal.offer_sal_to) as median_to,
	percentile_cont(0.5) within group (order by ofsal.offer_sal_from) as median_from 
from (select * from public.salary_uni_ranges(array['b2b'],'data')) ofsal
group by ofsal.experience_level
order by median_to desc;
```
#### salary disclosed % per seniority
```sql
select * from public.disclosure_share('experience_level','data',array['b2b'])
```

### SKILLS
#### most popular skills (per all offers)
```sql
with 
	skills_num_of_offers as (select * from public.attribute_share('skill_name',null,null,null)), 
	offers_num as (select total_num_of_offers from total_num_of_offers(1))
select 
	so.attribute, 
	round(so.offer_count*100/(select total_num_of_offers from offers_num) ,0) as share_in_total 
from skills_num_of_offers so order by share_in_total desc limit 20;
```
#### top skills per seniority and specjalization
```sql
with w2 as(
SELECT DISTINCT 
	skill_name,
	dense_rank() over (partition by skill_name order by jjit_id) + dense_rank() over (partition by skill_name order by jjit_id desc) - 1 as offer_count 
from vw_all_data 
where 
	marker_icon = 'data'
	and 
	experience_level = 'mid'
order by offer_count desc limit 6)
select 
	skill_name,
	offer_count*100/(SELECT count(distinct jjit_id) from vw_all_data where marker_icon = 'data'and experience_level = 'mid') as skill_share
	from w2;
```
### LOCALIZATIONS

#### check per country
```sql
select distinct 
		city,
		country,
		workplace_type,
		dense_rank() over (partition by city, workplace_type order by jjit_id) + dense_rank() over (partition by city,workplace_type  order by jjit_id desc) - 1 as offer_count
from vw_all_data
where city in ('Warszawa','Kraków','Wrocław') 
order by city
```