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
  (select value.string_value from unnest(event_params) where key ='firebase_event_origin') as event_origin, 
  (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'board') AS board,

  /* Level Analysis */
  (select value.double_value from unnest(event_params) where key ='level') as level_name,
  (select value.double_value from unnest(event_params) where key ='score') as score,
  /* Virtual Currency Analysis */ 
  (select value.string_value from unnest(event_params) where key ='virtual_currency_name') as virtual_currency_name,
  (select value.string_value from unnest(event_params) where key ='item_name') as item_name,

  /* In app purchase Analysis */ 
   (select value.string_value from unnest(event_params) where key ='product_id') as product_id,
   (select value.int_value from unnest(event_params) where key ='quantity') as quantity,
   event_value_in_usd as price,
   (select value.int_value from unnest(event_params) where key ='quantity')  * event_value_in_usd as revenue_usd,

  /* In app Ads and extra step after watching ads + Play type */ 
  (select value.string_value from unnest(user_properties) where key ='ad_frequency') as ad_frequency,
  (select value.string_value from unnest(user_properties) where key ='initial_extra_steps') as initial_extra_steps,
  CASE WHEN 
  (select value.string_value from unnest(user_properties) where key ='plays_quickplay') = 'true'
  THEN 'TRUE' ELSE 'FALSE'
  END AS plays_quickplay,
  CASE WHEN
  (select value.string_value from unnest(user_properties) where key ='plays_progressive') = 'true'
  THEN 'TRUE' ELSE 'FALSE'
  END AS plays_progressive,

  /* Firebase experience group count */ 
  CASE 
      WHEN (SELECT value.string_value FROM UNNEST(user_properties) WHERE key ='firebase_exp_1') >= '1' THEN 'firebase_exp_1'
      WHEN (SELECT value.string_value FROM UNNEST(user_properties) WHERE key ='firebase_exp_3') >= '1' THEN 'firebase_exp_3'
      WHEN (SELECT value.string_value FROM UNNEST(user_properties) WHERE key ='firebase_exp_4') >= '1' THEN 'firebase_exp_4'
      WHEN (SELECT value.string_value FROM UNNEST(user_properties) WHERE key ='firebase_exp_5') >= '1' THEN 'firebase_exp_5'
      WHEN (SELECT value.string_value FROM UNNEST(user_properties) WHERE key ='firebase_exp_7') >= '1' THEN 'firebase_exp_7'
      ELSE NULL 
  END AS firebase_exp_group,
  
  
   FROM
  `firebase-public-project.analytics_153293282.events_*`
