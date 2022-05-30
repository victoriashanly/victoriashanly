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
        on p.job_position_id = phj.position_id and p.listing_id=phj.listing_id
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
)

select
    pc.*,
    case when candidate_id in (select candidate_id from target_first_applications) then 1 else 0 end as recent
from proportional_candidate pc
