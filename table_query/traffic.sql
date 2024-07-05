WITH traffic AS (
SELECT
  event_date,
  user_pseudo_id AS user_id,
  CASE WHEN
  event_name = 'user_engagement'
  THEN user_pseudo_id
  END AS engaged_user_id, 

  traffic_source.source ||"/"|| traffic_source.medium AS traffic_name,
  traffic_source.source AS traffic_source,
  traffic_source.medium AS traffic_medium,
  device.category AS device_category,
  device.language AS device_language,
  geo.country, geo.city,
  app_info.id as info_id,
  app_info.version as app_version,
  app_info.install_store as install_store,
  app_info.install_source as install_source,

  (select value.string_value from unnest(event_params) where key ='firebase_screen_class') as screen_class,
  event_name,
  (select value.string_value from unnest(event_params) where key ='firebase_event_origin') as event_origin, 
  (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'board') AS board, 
  COUNTIF(event_name = 'screen_view') as screen_view, 

  CASE WHEN 
  event_name LIKE '%add_to_cart%'
  OR event_name LIKE '%add_to_wishlist%'
  OR event_name LIKE '%app_store_subscription_convert%'
  OR event_name LIKE '%app_store_subscription_renew%'
  OR event_name LIKE '%app_update%'
  OR event_name LIKE '%begin_checkout%'
  OR event_name LIKE '%completed_5_levels%'
  OR event_name LIKE '%first_open%'
  OR event_name LIKE '%purchase%'
  THEN 'TRUE'
  ELSE 'FALSE' 
  END AS is_conversion,


  FROM
  `firebase-public-project.analytics_153293282.events_*`
GROUP BY ALL
ORDER BY screen_view DESC 
) ,
time AS (
SELECT
  event_date,
  user_pseudo_id AS user_id,
   (select value.string_value from unnest(event_params) where key ='firebase_screen_class') as screen_class,
sum((select value.int_value from unnest(event_params) where key = 'engagement_time_msec'))/1000 as time_on_page,
(max(event_timestamp)-min(event_timestamp))/1000000 as time_on_session
FROM
  `firebase-public-project.analytics_153293282.events_*`
GROUP BY ALL)
SELECT t.*, u.time_on_page, u.time_on_session
FROM traffic t
LEFT JOIN time u ON u.user_id = t.user_id AND u.event_date = t.event_date AND t.screen_class = u.screen_class 
ORDER BY t.screen_view DESC 
