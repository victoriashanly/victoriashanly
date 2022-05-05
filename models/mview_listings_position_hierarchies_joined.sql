 SELECT 
    positions.id AS position_id,
    COALESCE(pp.name, positions.name) AS position_name,
    positions.subsidiary_id,
    COALESCE(pp.hierarchy_level_id, positions.hierarchy_level_id) AS hierarchy_level_id,
    ph1.id AS first_hierarchy_id,
    ph1.name AS first_hierarchy_name,
    ph2.id AS second_hierarchy_id,
    ph2.name AS second_hierarchy_name,
    ph3.id AS third_hierarchy_id,
    ph3.name AS third_hierarchy_name,
    ph4.id AS fourth_hierarchy_id,
    ph4.name AS fourth_hierarchy_name,
    l.listing_id
   FROM cbo.positions
     LEFT JOIN cbo.positions pp ON positions.mapped_to_position_id = pp.id AND NOT positions.is_active
     LEFT JOIN cbo.position_hierarchies ph1 ON COALESCE(pp.position_hierarchy_id, positions.position_hierarchy_id) = ph1.id
     LEFT JOIN cbo.position_hierarchies ph2 ON ph1.parent_id = ph2.id
     LEFT JOIN cbo.position_hierarchies ph3 ON ph2.parent_id = ph3.id
     LEFT JOIN cbo.position_hierarchies ph4 ON ph3.parent_id = ph4.id
     LEFT JOIN cbo.listing_positions l ON COALESCE(pp.id, positions.id) = l.position_id