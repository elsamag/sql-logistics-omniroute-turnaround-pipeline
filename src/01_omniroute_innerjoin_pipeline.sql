-- =============================================================================
-- Enterprise Practice: Elsamag IT Solutions
-- Author & Lead Technical Consultant: Samuel Chinwendu Agu
-- Client: OmniRoute Freight Systems | Domain: Logistics & Fleet Operations
-- Module: SQL Data Filtering & Relational Transformations (Concept 2: Inner Joins)
-- File Path: src/01_omniroute_innerjoin_pipeline.sql
-- Target Dialect: Google Cloud BigQuery Standard SQL
-- =============================================================================

CREATE OR REPLACE TABLE `omniroute_logistics.curated_dispatch_pipeline`
PARTITION BY DATE(dispatch_timestamp)
CLUSTER BY fleet_region, vehicle_class AS

WITH partitioned_trips AS (
  -- Step 1: Ingest dispatch events and prune date partitions to active operational window
  SELECT
    trip_id,
    driver_id,
    vehicle_id,
    origin_hub,
    destination_hub,
    delay_minutes,
    trip_status,
    dispatch_timestamp
  FROM
    `omniroute_logistics.dispatch_events_*`
  WHERE
    _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY))
                      AND FORMAT_DATE('%Y%m%d', CURRENT_DATE())
    AND trip_status IS NOT NULL
),

active_drivers AS (
  -- Step 2: Extract active driver master records and enforce SHA-256 PII hashing
  SELECT
    driver_id,
    TO_HEX(SHA256(LOWER(TRIM(driver_id)))) AS hashed_driver_token,
    driver_name,
    license_tier,
    fleet_region
  FROM
    `omniroute_logistics.master_drivers`
  WHERE
    is_active = TRUE
),

active_vehicles AS (
  -- Step 3: Extract serviceable fleet assets cleared for dispatch
  SELECT
    vehicle_id,
    vehicle_class,
    fuel_efficiency_mpg,
    telematics_status
  FROM
    `omniroute_logistics.fleet_inventory`
  WHERE
    maintenance_lock = FALSE
),

matched_relational_pipeline AS (
  -- Step 4: Execute strict relational inner joins across primary and foreign entity keys
  SELECT
    t.trip_id,
    d.hashed_driver_token,
    d.fleet_region,
    v.vehicle_id,
    v.vehicle_class,
    t.origin_hub,
    t.destination_hub,
    t.delay_minutes,
    t.trip_status,
    t.dispatch_timestamp,
    CURRENT_TIMESTAMP() AS processed_at
  FROM
    partitioned_trips AS t
  INNER JOIN
    active_drivers AS d
    ON t.driver_id = d.driver_id
  INNER JOIN
    active_vehicles AS v
    ON t.vehicle_id = v.vehicle_id
  WHERE
    t.trip_status IN ('Completed', 'In_Transit', 'Delayed')
)

-- Step 5: Final projection to partitioned and clustered destination table
SELECT
  trip_id,
  hashed_driver_token,
  fleet_region,
  vehicle_id,
  vehicle_class,
  origin_hub,
  destination_hub,
  delay_minutes,
  trip_status,
  dispatch_timestamp,
  processed_at
FROM
  matched_relational_pipeline;
