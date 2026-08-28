-- =============================================================================
-- Enterprise Practice: Elsamag IT Solutions
-- Author & Lead Technical Consultant: Samuel Chinwendu Agu
-- Repository Target: sql-logistics-omniroute-turnaround-pipeline
-- File: src/03_pii_sanitization_layer.sql
-- Dialect: Google Cloud BigQuery Standard SQL
-- Objective: Cryptographic SHA-256 sanitization and deterministic tokenization
--            of driver PII, telematics identifiers, and contact metadata.
-- =============================================================================

CREATE OR REPLACE VIEW `omniroute_logistics.v_sanitized_dispatch_stream` AS

WITH raw_driver_telemetry AS (
  SELECT
    driver_id,
    driver_name,
    driver_license_no,
    contact_phone,
    email_address,
    fleet_region,
    is_active
  FROM
    `omniroute_logistics.master_drivers`
  WHERE
    is_active = TRUE
),

raw_dispatch_events AS (
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

sanitized_driver_tokens AS (
  SELECT
    driver_id AS raw_driver_key,
    -- Deterministic SHA-256 Hashing for Zero-Exposure Join Alignment
    TO_HEX(SHA256(LOWER(TRIM(driver_id)))) AS hashed_driver_token,
    TO_HEX(SHA256(LOWER(TRIM(driver_license_no)))) AS hashed_license_token,
    -- Masked Contact Identity for Reporting Interfaces
    CONCAT(
      SUBSTR(SPLIT(driver_name, ' ')[OFFSET(0)], 1, 1), 
      '*** ', 
      SUBSTR(SPLIT(driver_name, ' ')[SAFE_OFFSET(1)], 1, 1), 
      '***'
    ) AS masked_driver_name,
    CONCAT('***-***-', SUBSTR(TRIM(contact_phone), -4)) AS masked_phone_suffix,
    CONCAT(
      SUBSTR(SPLIT(email_address, '@')[OFFSET(0)], 1, 2),
      '***@',
      SPLIT(email_address, '@')[OFFSET(1)]
    ) AS masked_email,
    fleet_region
  FROM
    raw_driver_telemetry
)

SELECT
  e.trip_id,
  d.hashed_driver_token,
  d.hashed_license_token,
  d.masked_driver_name,
  d.masked_phone_suffix,
  d.masked_email,
  d.fleet_region,
  e.vehicle_id,
  e.origin_hub,
  e.destination_hub,
  e.delay_minutes,
  e.trip_status,
  e.dispatch_timestamp,
  CURRENT_TIMESTAMP() AS sanitization_timestamp
FROM
  raw_dispatch_events AS e
INNER JOIN
  sanitized_driver_tokens AS d
  ON e.driver_id = d.raw_driver_key;
