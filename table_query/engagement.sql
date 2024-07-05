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
  event_name, 

  CASE WHEN 
  event_name LIKE '%add_to_cart%'
  OR event_name LIKE '%add_to_wishlist%'
  OR event_name LIKE '%app_store_subscription_convert%'
  OR event_name LIKE '%app_store_subscription_renew%'
  OR event_name LIKE '%app_update%'
  OR event_name LIKE '%begin_checkout%'
  OR event_name LIKE '%completed_5_levels%'
  OR event_name LIKE 'first_open%'
  OR event_name LIKE '%level_start%'
  OR event_name LIKE '%level_complete%'
  OR event_name LIKE 'level_end_quickplay%'
  OR event_name LIKE '%purchase%'
  THEN 'TRUE'
  ELSE 'FALSE' 
  END AS is_conversion,

  (select value.string_value from unnest(event_params) where key ='level_name') as level_name,
  (select value.double_value from unnest(event_params) where key ='level') as num_of_level,
  (select value.double_value from unnest(event_params) where key ='value') as level_up_point,
  (select value.double_value from unnest(event_params) where key ='time') as time,
  (select value.double_value from unnest(event_params) where key ='score') as score,
  (select value.string_value from unnest(event_params) where key ='virtual_currency_name') as virtual_currency_name,
  (select value.string_value from unnest(event_params) where key ='item_name') as item_name,




  (select value.string_value from unnest(user_properties) where key ='ad_frequency') as ad_frequency,
  (select value.string_value from unnest(user_properties) where key ='initial_extra_steps') as initial_extra_steps,
  (select value.string_value from unnest(user_properties) where key ='plays_quickplay') as plays_quickplay,
  (select value.string_value from unnest(user_properties) where key ='plays_progressive') as plays_progressive,
  (select value.string_value from unnest(user_properties) where key ='num_levels_available') as num_levels_available,
  (select value.string_value from unnest(user_properties) where key ='firebase_exp_1') as firebase_exp_1,
  (select value.string_value from unnest(user_properties) where key ='firebase_exp_3') as firebase_exp_3,
  (select value.string_value from unnest(user_properties) where key ='firebase_exp_4') as firebase_exp_4,
  (select value.string_value from unnest(user_properties) where key ='firebase_exp_5') as firebase_exp_5,
  (select value.string_value from unnest(user_properties) where key ='firebase_exp_7') as firebase_exp_7,
  
  
   FROM
  `firebase-public-project.analytics_153293282.events_*`
