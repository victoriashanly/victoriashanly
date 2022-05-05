SELECT sl.id AS subsidiary_location_id,
    countries.value AS country,
    states.value AS state,
    divisions.value AS division,
    regions.value AS region,
    cities.value AS city,
    sites.value AS site,
    zones.value AS zone,
    fields.value AS field,
    markets.value AS market,
    recruiter_names.value AS recruiter_name,
    recruiter_ids.value AS recruiter_id,
    brands.value AS brand,
    formato.value AS format
   FROM cbo.subsidiary_locations sl
     LEFT JOIN ( SELECT DISTINCT slt.subsidiary_location_id,
            slt.value
           FROM cbo.subsidiary_tags st
             JOIN cbo.subsidiary_location_tags slt ON st.id = slt.subsidiary_tag_id
          WHERE lower(st.label::text) = 'país'::text) countries ON sl.id = countries.subsidiary_location_id
     LEFT JOIN ( SELECT DISTINCT slt.subsidiary_location_id,
            slt.value
           FROM cbo.subsidiary_tags st
             JOIN cbo.subsidiary_location_tags slt ON st.id = slt.subsidiary_tag_id
          WHERE lower(st.label::text) = ANY (ARRAY['estado'::text, 'state'::text])) states ON sl.id = states.subsidiary_location_id
     LEFT JOIN ( SELECT DISTINCT slt.subsidiary_location_id,
            slt.value
           FROM cbo.subsidiary_tags st
             JOIN cbo.subsidiary_location_tags slt ON st.id = slt.subsidiary_tag_id
          WHERE lower(st.label::text) = 'división'::text) divisions ON sl.id = divisions.subsidiary_location_id
     LEFT JOIN ( SELECT DISTINCT slt.subsidiary_location_id,
            slt.value
           FROM cbo.subsidiary_tags st
             JOIN cbo.subsidiary_location_tags slt ON st.id = slt.subsidiary_tag_id
          WHERE lower(st.label::text) = 'región'::text) regions ON sl.id = regions.subsidiary_location_id
     LEFT JOIN ( SELECT DISTINCT slt.subsidiary_location_id,
            slt.value
           FROM cbo.subsidiary_tags st
             JOIN cbo.subsidiary_location_tags slt ON st.id = slt.subsidiary_tag_id
          WHERE lower(st.label::text) = ANY (ARRAY['ciudad'::text, 'city'::text])) cities ON sl.id = cities.subsidiary_location_id
     LEFT JOIN ( SELECT DISTINCT slt.subsidiary_location_id,
            slt.value
           FROM cbo.subsidiary_tags st
             JOIN cbo.subsidiary_location_tags slt ON st.id = slt.subsidiary_tag_id
          WHERE lower(st.label::text) = 'site'::text) sites ON sl.id = sites.subsidiary_location_id
     LEFT JOIN ( SELECT DISTINCT slt.subsidiary_location_id,
            slt.value
           FROM cbo.subsidiary_tags st
             JOIN cbo.subsidiary_location_tags slt ON st.id = slt.subsidiary_tag_id
          WHERE lower(st.label::text) = ANY (ARRAY['zona'::text, 'zone'::text, 'plaza'::text])) zones ON sl.id = zones.subsidiary_location_id
     LEFT JOIN ( SELECT DISTINCT slt.subsidiary_location_id,
            slt.value
           FROM cbo.subsidiary_tags st
             JOIN cbo.subsidiary_location_tags slt ON st.id = slt.subsidiary_tag_id
          WHERE lower(st.label::text) = 'campo'::text) fields ON sl.id = fields.subsidiary_location_id
     LEFT JOIN ( SELECT DISTINCT slt.subsidiary_location_id,
            slt.value
           FROM cbo.subsidiary_tags st
             JOIN cbo.subsidiary_location_tags slt ON st.id = slt.subsidiary_tag_id
          WHERE lower(st.label::text) = 'mercado'::text) markets ON sl.id = markets.subsidiary_location_id
     LEFT JOIN ( SELECT DISTINCT slt.subsidiary_location_id,
            slt.value
           FROM cbo.subsidiary_tags st
             JOIN cbo.subsidiary_location_tags slt ON st.id = slt.subsidiary_tag_id
          WHERE lower(st.label::text) = ANY (ARRAY['reclutador a cargo'::text, 'reclutador'::text])) recruiter_names ON sl.id = recruiter_names.subsidiary_location_id
     LEFT JOIN ( SELECT DISTINCT slt.subsidiary_location_id,
            slt.value
           FROM cbo.subsidiary_tags st
             JOIN cbo.subsidiary_location_tags slt ON st.id = slt.subsidiary_tag_id
          WHERE lower(st.label::text) = 'recruiter_id'::text) recruiter_ids ON sl.id = recruiter_ids.subsidiary_location_id
     LEFT JOIN ( SELECT DISTINCT slt.subsidiary_location_id,
            slt.value
           FROM cbo.subsidiary_tags st
             JOIN cbo.subsidiary_location_tags slt ON st.id = slt.subsidiary_tag_id
          WHERE lower(st.label::text) = 'brand'::text) brands ON sl.id = brands.subsidiary_location_id
     LEFT JOIN ( SELECT DISTINCT slt.subsidiary_location_id,
            slt.value
           FROM cbo.subsidiary_tags st
             JOIN cbo.subsidiary_location_tags slt ON st.id = slt.subsidiary_tag_id
          WHERE lower(st.label::text) = 'formato'::text) formato ON sl.id = formato.subsidiary_location_id
  ORDER BY sl.id