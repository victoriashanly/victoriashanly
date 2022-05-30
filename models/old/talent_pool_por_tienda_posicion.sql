{{config(
    materialized="table"
)}}

-- TIENDA | ESTADO | CIUDAD | SOLICITUDES ABIERTAS| # CANDIDATOS | # CANDIDATOS PROPORCIONALES

with
listings as (
    select 
        listings.id, 
        listings.name 
    from {{source ('cbo','listings')}}
    inner join {{source ('cbo','subsidiaries')}} 
        on listings.subsidiary_id = subsidiaries.id
    where listing_type in ('JOB')
    and subsidiaries.id =51
),
steps as (
    select 
        id as step_id, 
        name, 
        type, 
        listing_id
    from {{source ('cbo','steps')}} s
    where 
        listing_id in (select id from listings)
        and s.type='INTERVIEW'
),

target_first_applications as 
(
    select 
        listing_id,
        n_candidate_id,
        candidate_id
    from intermediate.funnel
    where
        listing_id in (select id from listings)
        and application_date::date >= current_date - interval '30 days'
        and application_date::date <= current_date
        and application_date is not null
),

mview_listings_position_hierarchies_joined as
(
    select * from {{ref('stg_mview_listings_position_hierarchies_joined')}} 
),

mview_subsidiary_locations_structured_metadata as
(
    select * from {{ref('stg_mview_subsidiary_locations_structured_metadata')}}
),

requisitions as
(
    select * from {{ref('stg_requisitions')}}
)
,
candidate_steps as (
    select 
        f.step_id, 
        f.state, 
        f.candidate_id,
        f.listing_id,
        lcwp.working_place_id as subsidiary_locations_id,
        p.job_position_id as position_id,
        phj.position_name
    from bi.listing_candidates_last_step_state f
    left join bi.listing_candidate_current_working_places lcwp 
        on f.listing_id = lcwp.listing_id and f.candidate_id = lcwp.candidate_id
    left join bi.listing_candidate_current_job_positions p
        on f.listing_id = p.listing_id and f.candidate_id = p.candidate_id
    left join mview_listings_position_hierarchies_joined phj
        on coalesce(p.job_position_id) = phj.position_id and p.listing_id=phj.listing_id
    where f.listing_id in (select id from listings)
    and f.step_id in (select step_id from steps)
    and f.state in ('STARTED','FINISHED','RESTARTED')
),

proportional_candidate as
(
select 
    cs.*,
    1.000/greatest(count((listing_id,subsidiary_locations_id,position_id)) over (partition by cs.candidate_id),1) as proportional_candidate
from candidate_steps cs
),

proportional_candidate_filtered as
(
select 
    cs.*,
    1.000/greatest(count((listing_id,subsidiary_locations_id)) over (partition by cs.candidate_id),1) as proportional_candidate_filtered
from candidate_steps cs
where candidate_id in (select candidate_id from target_first_applications)
),

-- acá debería hacer una tabla aparte porque el count distinct de candidate_id abierto por listing, no es lo mismo que no abierto -- 

candidates_by_store as 
(   

select 
    listing_id,
    subsidiary_locations_id,
    position_id,
    count(distinct candidate_id) as candidatos,
    round(sum(proportional_candidate),1) as candidatos_proporcionales
from proportional_candidate
group by 1,2
),

candidates_by_store_filtered as 
(   

select
    listing_id,
    subsidiary_locations_id,
    position_id,
    count(distinct candidate_id) as candidatos_filtered,
    round(sum(proportional_candidate_filtered),1) as candidatos_proporcionales_filtered
    --array_agg(distinct candidate_id) as candidatos_array
from proportional_candidate_filtered
group by 1,2,3
),

candidates_by_store_total as
(
select
    cs.listing_id,
    cs.subsidiary_locations_id,
    cs.position_id,
    candidatos,
    candidatos_proporcionales,
    candidatos_filtered,
    candidatos_proporcionales_filtered
from candidates_by_store cs
left join candidates_by_store_filtered csf
on (cs.subsidiary_locations_id = csf.subsidiary_locations_id
    or (cs.subsidiary_locations_id is null and csf.subsidiary_locations_id is null))
and (cs.position_id = csf.position_id)
    --or (cs.position_id is null and csf.position_id is null))
)
-- acá termina la tabla y lo de abajo debería ser el select que hago en metabase, con filtros de listing para la tabla de arriba
select
    distinct
    cs.listing_id,
    cs.position_id,
    cs.subsidiary_locations_id,
    coalesce( subsidiary_locations.name,'Sin Tienda') as location_name,
    phj.position_name as position_name,
    sl.state as state,
    sl.city as city,
    coalesce(candidatos,0) as candidates,
    coalesce(candidatos_proporcionales,0) as proportional_candidates,
    coalesce(candidatos_filtered,0) as recent_candidates,
    coalesce(candidatos_proporcionales_filtered,0) as recent_proportional_candidates
from candidates_by_store_total cs
left join {{source ('cbo','subsidiary_locations')}}
        on cs.subsidiary_locations_id = subsidiary_locations.id
left join mview_subsidiary_locations_structured_metadata sl
        on subsidiary_locations.id = sl.subsidiary_location_id
left join mview_listings_position_hierarchies_joined phj
        on cs.position_id = phj.position_id
where true
order by coalesce(candidatos_proporcionales,0) asc
