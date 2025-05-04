DROP TABLE IF EXISTS t_flow_per_nextgen;

CREATE TABLE t_flow_per_nextgen AS
WITH unique_ids AS (
    SELECT DISTINCT nextgen_id
    FROM s_flood_inundation_ar
),
crosswalked AS (
    SELECT u.nextgen_id, x.feature_id
    FROM unique_ids u
    JOIN t_nextgen_to_nwm x ON u.nextgen_id = x.nextgen_id
),
flows_with_array AS (
    SELECT
        c.nextgen_id,
        f.feature_id,
        f.model_run_time,
        ARRAY[
            flow_t00, flow_t01, flow_t02, flow_t03, flow_t04, flow_t05,
            flow_t06, flow_t07, flow_t08, flow_t09, flow_t10, flow_t11,
            flow_t12, flow_t13, flow_t14, flow_t15, flow_t16, flow_t17
        ] AS flow_array
    FROM crosswalked c
    JOIN t_flow_forecast f ON c.feature_id = f.feature_id
)
SELECT
    nextgen_id,
    feature_id,
    model_run_time,
    flow_array,
    (
        SELECT MAX(val) FROM unnest(flow_array) AS val
    ) AS max_flow,
    (
        SELECT i - 1  -- Subtract 1 because PostgreSQL arrays are 1-based
        FROM generate_subscripts(flow_array, 1) AS i
        WHERE flow_array[i] = (
            SELECT MAX(val) FROM unnest(flow_array) AS val
        )
        LIMIT 1  -- in case of ties, take the first occurrence
    ) AS max_hour
FROM flows_with_array;

/*
Method to comment out multiple rows
*/

/*
-- temporary fake flow for testing
UPDATE t_flow_per_nextgen
SET
   flow_array[14] = 925,
   flow_array[15] = 1850,
   flow_array[16] = 925,
   max_flow = 1850,
   max_hour = 15;
-- end temp
*/

DROP TABLE IF EXISTS s_selected_flood_ar;

CREATE TABLE s_selected_flood_ar AS
SELECT 
    t.nextgen_id,
    t.max_flow,
    t.model_run_time,
	t.max_hour,
    s.flow,
    s.geometry
FROM t_flow_per_nextgen t
LEFT JOIN LATERAL (
    SELECT *
    FROM s_flood_inundation_ar s
    WHERE s.nextgen_id = t.nextgen_id
      AND s.flow <= t.max_flow
    ORDER BY s.flow DESC
    LIMIT 1
) s ON true;

DROP TABLE IF EXISTS s_flood_road_ln;

CREATE TABLE s_flood_road_ln AS
WITH below_trigger_roads AS (
    SELECT 
        ft.*, 
        mf.max_flow, 
        mf.model_run_time
    FROM 
        t_road_flood_trigger ft
    LEFT JOIN 
        t_flow_per_nextgen mf 
        ON ft.nextgen_id = mf.nextgen_id
    WHERE 
        ft.min_flood_flow < mf.max_flow
)
SELECT 
    s.*, 
    btr.nextgen_id, 
    btr.min_flood_flow, 
    btr.max_flow, 
    btr.model_run_time
FROM 
    below_trigger_roads btr
JOIN 
    s_road_segment_ln s 
    ON btr.road_id = s.road_id;
	
DROP TABLE IF EXISTS s_flood_merge_ar;

CREATE TABLE s_flood_merge_ar AS
SELECT
    -- Take the model_run_time from the first record (by order, or just any one)
    (SELECT model_run_time FROM s_selected_flood_ar ORDER BY model_run_time LIMIT 1) AS model_run_time,
    ST_Multi(ST_Union(geometry)) AS geometry
FROM s_selected_flood_ar;

DROP TABLE IF EXISTS s_flood_road_trim_ln;

CREATE TABLE s_flood_road_trim_ln AS
SELECT
    r.osm_id,
    r.fclass,
    r.name,
    r.ref,
    r.road_id,
    r.nextgen_id,
    r.min_flood_flow,
    r.max_flow,
    r.model_run_time,
    ST_Intersection(r.geometry, f.geometry) AS geometry
FROM
    s_flood_road_ln r
JOIN
    s_flood_merge_ar f
ON
    ST_Intersects(r.geometry, f.geometry)
WHERE
    ST_Intersects(r.geometry, f.geometry);
	
-- creating / updating the t_current_forecast
DROP TABLE IF EXISTS t_current_forecast;

-- Create the new table with a single row containing the first model_run_time value from t_flow_forecast
CREATE TABLE t_current_forecast AS
SELECT model_run_time
FROM t_flow_forecast
LIMIT 1;