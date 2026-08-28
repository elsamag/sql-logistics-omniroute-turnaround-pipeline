-- ============================================================================
-- Enterprise Practice: Elsamag IT Solutions
-- Author & Lead Technical Consultant: Samuel Chinwendu Agu
-- Client: OmniRoute Freight Systems | Domain: Logistics & Fleet Operations
-- Asset: src/02_incident_failover_quarantine.sql
-- Dialect: Google Cloud BigQuery Standard SQL
-- Architecture: Dynamic Failover Quarantine & Automated Rollback Matrix (Add-On 6)
-- ============================================================================

BEGIN
  -- -------------------------------------------------------------------------
  -- STAGE 1: Partitioned Staging Ingestion & Unmatched Entity Identification
  -- -------------------------------------------------------------------------
  CREATE OR REPLACE TEMP TABLE _staging_dispatch_audit AS
  WITH active_window_trips AS (
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
  ),
  
  driver_lookup AS (
    SELECT driver_id, is_active
    FROM `omniroute_logistics.master_drivers`
  ),
  
  vehicle_lookup AS (
    SELECT vehicle_id, maintenance_lock
    FROM `omniroute_logistics.fleet_inventory`
  )
  
  SELECT
    t.trip_id,
    t.driver_id,
    t.vehicle_id,
    t.origin_hub,
    t.destination_hub,
    t.delay_minutes,
    t.trip_status,
    t.dispatch_timestamp,
    CASE
      WHEN t.trip_id IS NULL THEN 'CORRUPTED_NULL_TRIP_ID'
      WHEN d.driver_id IS NULL THEN 'UNLINKED_ORPHAN_DRIVER'
      WHEN d.is_active = FALSE THEN 'INACTIVE_DRIVER_ASSIGNMENT'
      WHEN v.vehicle_id IS NULL THEN 'UNLINKED_ORPHAN_VEHICLE'
      WHEN v.maintenance_lock = TRUE THEN 'MAINTENANCE_LOCKED_VEHICLE'
      WHEN t.origin_hub IS NULL OR t.destination_hub IS NULL THEN 'MISSING_ROUTE_GEOMETRY'
      ELSE 'CLEAN'
    END AS triage_status
  FROM
    active_window_trips AS t
  LEFT JOIN
    driver_lookup AS d
    ON t.driver_id = d.driver_id
  LEFT JOIN
    vehicle_lookup AS v
    ON t.vehicle_id = v.vehicle_id;

  -- -------------------------------------------------------------------------
  -- STAGE 2: Automated Quarantine Diversion (Tier 1 & Tier 2 Failures)
  -- -------------------------------------------------------------------------
  INSERT INTO `omniroute_quarantine.failed_trips`[span_0](start_span)[span_0](end_span) (
    trip_id,
    driver_id,
    vehicle_id,
    origin_hub,
    destination_hub,
    delay_minutes,
    trip_status,
    dispatch_timestamp,
    failure_reason,
    quarantined_at
  )
  SELECT
    trip_id,
    driver_id,
    vehicle_id,
    origin_hub,
    destination_hub,
    delay_minutes,
    trip_status,
    dispatch_timestamp,
    triage_status AS failure_reason,
    CURRENT_TIMESTAMP() AS quarantined_at[span_1](start_span)[span_1](end_span)
  FROM
    _staging_dispatch_audit
  WHERE
    triage_status != 'CLEAN';

  -- -------------------------------------------------------------------------
  -- STAGE 3: Critical Error Threshold Assertion (Failover Trigger Gate)
  -- -------------------------------------------------------------------------
  -- Halt sync and trigger failover if anomaly rate exceeds 0.5% of batch
  IF (
    SELECT 
      SAFE_DIVIDE(COUNTIF(triage_status != 'CLEAN'), COUNT(*))
    FROM 
      _staging_dispatch_audit
  ) > 0.005 THEN
    RAISE USING MESSAGE = 'CRITICAL: Quarantine breach — anomaly rate exceeded 0.5% threshold. Production promotion aborted.';
  END IF;

  -- -------------------------------------------------------------------------
  -- STAGE 4: Atomic Production Table Promotion
  -- -------------------------------------------------------------------------
  CREATE OR REPLACE TABLE `omniroute_logistics.curated_dispatch_live`
  PARTITION BY DATE(dispatch_timestamp)
  CLUSTER BY origin_hub, destination_hub AS
  SELECT
    trip_id,
    TO_HEX(SHA256(LOWER(TRIM(driver_id)))) AS hashed_driver_token,
    vehicle_id,
    origin_hub,
    destination_hub,
    delay_minutes,
    trip_status,
    dispatch_timestamp,
    CURRENT_TIMESTAMP() AS validated_at
  FROM
    _staging_dispatch_audit
  WHERE
    triage_status = 'CLEAN';

  -- Log successful health check execution
  INSERT INTO `omniroute_audit.incident_failover_log`[span_2](start_span)[span_2](end_span) (
    incident_timestamp,
    execution_status,
    total_records_evaluated,
    quarantined_count,
    promoted_count,
    error_details
  )
  SELECT
    CURRENT_TIMESTAMP(),[span_3](start_span)[span_3](end_span)
    'SUCCESS',
    COUNT(*),
    COUNTIF(triage_status != 'CLEAN'),
    COUNTIF(triage_status = 'CLEAN'),
    'Batch passed all relational integrity and quarantine assertions.'
  FROM
    _staging_dispatch_audit;

EXCEPTION WHEN ERROR THEN[span_4](start_span)[span_4](end_span)
  -- -------------------------------------------------------------------------
  -- STAGE 5: Disaster Recovery Automated Snapshot Rollback (Tier 3 Failover)
  -- -------------------------------------------------------------------------
  INSERT INTO `omniroute_audit.incident_failover_log` ([span_5](start_span)[span_5](end_span)
    incident_timestamp,
    execution_status,
    total_records_evaluated,
    quarantined_count,
    promoted_count,
    error_details
  )
  VALUES (
    CURRENT_TIMESTAMP(),[span_6](start_span)[span_6](end_span)
    'FAILED_ROLLBACK_INITIATED',
    NULL,
    NULL,
    NULL,
    @@error.message[span_7](start_span)[span_7](end_span)
  );

  -- Rollback live dataset pointer to verified immutable snapshot[span_8](start_span)[span_8](end_span)
  RESTORE `omniroute_logistics.curated_dispatch_live`[span_9](start_span)[span_9](end_span)
  FROM SNAPSHOT `omniroute_snapshots.curated_dispatch_live_t_minus_1`;[span_10](start_span)[span_10](end_span)

END;
