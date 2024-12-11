-- Raw table: Unprocessed data with essential fields cleaned and normalized.
CREATE TABLE `trangthu.raw.raw_events` AS
SELECT 
  PARSE_DATE('%Y%m%d', event_date) AS event_date,
  event_timestamp,
  user_pseudo_id AS user_id,
  event_name,
  ep.key AS event_key,
  ep.value.string_value AS event_string_value,
  ep.value.int_value AS event_int_value,
  ep.value.float_value AS event_float_value,
  ep.value.double_value AS event_double_value,
  event_previous_timestamp,
  user_first_touch_timestamp,
  device.operating_system,
  device.language,
  geo.continent, 
  geo.country,
  app_info.version AS app_version,
  traffic_source.medium AS traffic_source_medium,
  traffic_source.source AS traffic_source_source,
  platform
FROM `firebase-public-project.analytics_153293282.events_*`,
  UNNEST (event_params) AS ep


  
-- Transform table: Processed data with calculated indicators and simplified structure. 
CREATE TABLE `trangthu.transform.users_and_revenue` AS 
WITH 
  t1 AS 
  (
    SELECT DISTINCT
      event_date, 
      user_id,
      country,
      app_version,
    FROM `trangthu.raw.raw_events`
  ),
  r1 AS 
  (
    SELECT DISTINCT
      event_date,
      user_id,
      country,
      app_version, 
    FROM `trangthu.raw.raw_events`
    WHERE event_name = 'app_remove'
  ), 
  n1 AS
  (
    SELECT DISTINCT
      event_date,
      user_id,
      country,
      app_version
    FROM `trangthu.raw.raw_events`
    WHERE event_name = 'first_open'
  ),
  rev1 AS
  (
    SELECT 
      event_date,
      user_id,
      country, 
      app_version,
      SUM(event_int_value) AS revenue, 
      SUM(IF(event_name = 'ad_reward', event_int_value, 0)) AS ad_revenue,
      SUM(IF(event_name = 'in_app_purchase', event_int_value, 0)) AS iap_revenue
    FROM `trangthu.raw.raw_events`
    WHERE event_name in ('ad_reward', 'in_app_purchase')
      AND event_key = 'value'
    GROUP BY 
      event_date,
      user_id,
      country,
      app_version
  ), 
  final AS 
  (
    SELECT 
      t1.*, 
      IF(r1.user_id IS NOT NULL, TRUE, FALSE) AS is_remove_user, 
      IF(n1.user_id IS NOT NULL, TRUE, FALSE) AS is_new_user,
      IFNULL(rev1.revenue, 0) AS revenue,
      IFNULL(rev1.ad_revenue, 0) AS ad_revenue,
      IFNULL(rev1.iap_revenue, 0) AS iap_revenue
    FROM t1 
    LEFT JOIN r1 
      USING(event_date, user_id, country, app_version)
    LEFT JOIN n1 
      USING(event_date, user_id, country, app_version)
    LEFT JOIN rev1 
      USING(event_date, user_id, country, app_version)
  )
SELECT * 
FROM final 


  
-- Aggregated metrics for reporting based on key dimensions. 
SELECT 
  event_date,
  country,
  app_version,
  COUNT(DISTINCT user_id) AS num_active_users,
  COUNT(DISTINCT IF(is_new_user is true, user_id, null)) AS num_new_users,
  COUNT(DISTINCT IF(is_remove_user is true, user_id, null)) AS num_remove_users,
  SUM(revenue) AS revenue, 
  SUM(ad_revenue) AS ad_revenue,
  SUM(iap_revenue) AS iap_revenue
FROM `trangthu.transform.users_and_revenue`
GROUP BY 
  event_date,
  country,
  app_version
