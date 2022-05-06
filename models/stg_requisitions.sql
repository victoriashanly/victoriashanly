with
listings as (
    select 
        listings.id, 
        listings.name 
    from cbo.listings
    inner join cbo.subsidiaries 
        on listings.subsidiary_id = subsidiaries.id
    where listing_type in ('JOB')
    and subsidiaries.id =51
)
select
    working_location_id,
    r.position_id,
    phj.position_name,
    sum(openings) as solicitudes 
from candidate_interview.requisitions r
left join dbt_victoriashanly.mview_listings_position_hierarchies_joined phj
    on r.position_id = phj.position_id and r.listing_id=phj.listing_id
where r.listing_id in (select id from listings)
    and closed='false'
group by 1,2,3