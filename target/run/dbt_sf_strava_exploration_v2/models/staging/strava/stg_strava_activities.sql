-- back compat for old kwarg name
  
  begin;
    
        
            
            
        
    

    

    merge into STRAVA_STAGING.staging.stg_strava_activities as DBT_INTERNAL_DEST
        using STRAVA_STAGING.staging.stg_strava_activities__dbt_tmp as DBT_INTERNAL_SOURCE
        on (
                DBT_INTERNAL_SOURCE.activity_key = DBT_INTERNAL_DEST.activity_key
            )

    
    when matched then update set
        "ACTIVITY_KEY" = DBT_INTERNAL_SOURCE."ACTIVITY_KEY","START_DATE_KEY" = DBT_INTERNAL_SOURCE."START_DATE_KEY","ACTIVITY_ID" = DBT_INTERNAL_SOURCE."ACTIVITY_ID","ACTIVITY_START_TIMESTAMP_NTZ" = DBT_INTERNAL_SOURCE."ACTIVITY_START_TIMESTAMP_NTZ","ACTIVITY_NAME" = DBT_INTERNAL_SOURCE."ACTIVITY_NAME","SPORT" = DBT_INTERNAL_SOURCE."SPORT","IS_MANUAL" = DBT_INTERNAL_SOURCE."IS_MANUAL","HAS_HEARTRATE" = DBT_INTERNAL_SOURCE."HAS_HEARTRATE","HAS_POWER" = DBT_INTERNAL_SOURCE."HAS_POWER","IS_INDOOR" = DBT_INTERNAL_SOURCE."IS_INDOOR","IS_RACE" = DBT_INTERNAL_SOURCE."IS_RACE","AVERAGE_TEMPATURE_C" = DBT_INTERNAL_SOURCE."AVERAGE_TEMPATURE_C","MIN_ELEVATION_M" = DBT_INTERNAL_SOURCE."MIN_ELEVATION_M","MAX_ELEVATION_M" = DBT_INTERNAL_SOURCE."MAX_ELEVATION_M","ELEVATION_GAIN_M" = DBT_INTERNAL_SOURCE."ELEVATION_GAIN_M","AVERAGE_CADENCE_RPM" = DBT_INTERNAL_SOURCE."AVERAGE_CADENCE_RPM","DISTANCE_M" = DBT_INTERNAL_SOURCE."DISTANCE_M","ELAPSED_TIME_S" = DBT_INTERNAL_SOURCE."ELAPSED_TIME_S","MOVING_TIME_S" = DBT_INTERNAL_SOURCE."MOVING_TIME_S","CALORIES_KJ" = DBT_INTERNAL_SOURCE."CALORIES_KJ","AVERAGE_HEARTRATE_BPM" = DBT_INTERNAL_SOURCE."AVERAGE_HEARTRATE_BPM","MAX_HEARTRATE_BPM" = DBT_INTERNAL_SOURCE."MAX_HEARTRATE_BPM","SUFFER_SCORE" = DBT_INTERNAL_SOURCE."SUFFER_SCORE","AVERAGE_SPEED_MS" = DBT_INTERNAL_SOURCE."AVERAGE_SPEED_MS","MAX_SPEED_MS" = DBT_INTERNAL_SOURCE."MAX_SPEED_MS","AVERAGE_POWER_WATTS" = DBT_INTERNAL_SOURCE."AVERAGE_POWER_WATTS","MAX_POWER_WATTS" = DBT_INTERNAL_SOURCE."MAX_POWER_WATTS","NORMALISED_POWER_WATTS" = DBT_INTERNAL_SOURCE."NORMALISED_POWER_WATTS","EXTRACTED_TIMESTAMP_UTC" = DBT_INTERNAL_SOURCE."EXTRACTED_TIMESTAMP_UTC","LOADED_TIMESTAMP_UTC" = DBT_INTERNAL_SOURCE."LOADED_TIMESTAMP_UTC","RECORD_SOURCE" = DBT_INTERNAL_SOURCE."RECORD_SOURCE"
    

    when not matched then insert
        ("ACTIVITY_KEY", "START_DATE_KEY", "ACTIVITY_ID", "ACTIVITY_START_TIMESTAMP_NTZ", "ACTIVITY_NAME", "SPORT", "IS_MANUAL", "HAS_HEARTRATE", "HAS_POWER", "IS_INDOOR", "IS_RACE", "AVERAGE_TEMPATURE_C", "MIN_ELEVATION_M", "MAX_ELEVATION_M", "ELEVATION_GAIN_M", "AVERAGE_CADENCE_RPM", "DISTANCE_M", "ELAPSED_TIME_S", "MOVING_TIME_S", "CALORIES_KJ", "AVERAGE_HEARTRATE_BPM", "MAX_HEARTRATE_BPM", "SUFFER_SCORE", "AVERAGE_SPEED_MS", "MAX_SPEED_MS", "AVERAGE_POWER_WATTS", "MAX_POWER_WATTS", "NORMALISED_POWER_WATTS", "EXTRACTED_TIMESTAMP_UTC", "LOADED_TIMESTAMP_UTC", "RECORD_SOURCE")
    values
        ("ACTIVITY_KEY", "START_DATE_KEY", "ACTIVITY_ID", "ACTIVITY_START_TIMESTAMP_NTZ", "ACTIVITY_NAME", "SPORT", "IS_MANUAL", "HAS_HEARTRATE", "HAS_POWER", "IS_INDOOR", "IS_RACE", "AVERAGE_TEMPATURE_C", "MIN_ELEVATION_M", "MAX_ELEVATION_M", "ELEVATION_GAIN_M", "AVERAGE_CADENCE_RPM", "DISTANCE_M", "ELAPSED_TIME_S", "MOVING_TIME_S", "CALORIES_KJ", "AVERAGE_HEARTRATE_BPM", "MAX_HEARTRATE_BPM", "SUFFER_SCORE", "AVERAGE_SPEED_MS", "MAX_SPEED_MS", "AVERAGE_POWER_WATTS", "MAX_POWER_WATTS", "NORMALISED_POWER_WATTS", "EXTRACTED_TIMESTAMP_UTC", "LOADED_TIMESTAMP_UTC", "RECORD_SOURCE")

;
    commit;