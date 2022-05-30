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
    listing_id,
    working_location_id,
    r.position_id,
    sum(openings) as solicitudes 
from candidate_interview.requisitions r
where closed='false'
group by 1,2,3