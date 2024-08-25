-- back compat for old kwarg name
  
  begin;
    
        
            
            
        
    

    

    merge into STRAVA_DEV.staging.stg_strava_activity_streams as DBT_INTERNAL_DEST
        using STRAVA_DEV.staging.stg_strava_activity_streams__dbt_tmp as DBT_INTERNAL_SOURCE
        on (
                DBT_INTERNAL_SOURCE.activity_stream_key = DBT_INTERNAL_DEST.activity_stream_key
            )

    
    when matched then update set
        "ACTIVITY_STREAM_KEY" = DBT_INTERNAL_SOURCE."ACTIVITY_STREAM_KEY","ACTIVITY_ID" = DBT_INTERNAL_SOURCE."ACTIVITY_ID","ELAPSED_TIME_S" = DBT_INTERNAL_SOURCE."ELAPSED_TIME_S","IS_MOVING" = DBT_INTERNAL_SOURCE."IS_MOVING","LATITUDE" = DBT_INTERNAL_SOURCE."LATITUDE","LONGITUDE" = DBT_INTERNAL_SOURCE."LONGITUDE","TEMPERATURE_C" = DBT_INTERNAL_SOURCE."TEMPERATURE_C","GRADE_PERCENT" = DBT_INTERNAL_SOURCE."GRADE_PERCENT","ELEVATION_M" = DBT_INTERNAL_SOURCE."ELEVATION_M","CADENCE_RPM" = DBT_INTERNAL_SOURCE."CADENCE_RPM","DISTANCE_M" = DBT_INTERNAL_SOURCE."DISTANCE_M","HEARTRATE_BPM" = DBT_INTERNAL_SOURCE."HEARTRATE_BPM","SPEED_MS" = DBT_INTERNAL_SOURCE."SPEED_MS","POWER_WATTS" = DBT_INTERNAL_SOURCE."POWER_WATTS","EXTRACTED_TIMESTAMP" = DBT_INTERNAL_SOURCE."EXTRACTED_TIMESTAMP","RECORD_SOURCE" = DBT_INTERNAL_SOURCE."RECORD_SOURCE"
    

    when not matched then insert
        ("ACTIVITY_STREAM_KEY", "ACTIVITY_ID", "ELAPSED_TIME_S", "IS_MOVING", "LATITUDE", "LONGITUDE", "TEMPERATURE_C", "GRADE_PERCENT", "ELEVATION_M", "CADENCE_RPM", "DISTANCE_M", "HEARTRATE_BPM", "SPEED_MS", "POWER_WATTS", "EXTRACTED_TIMESTAMP", "RECORD_SOURCE")
    values
        ("ACTIVITY_STREAM_KEY", "ACTIVITY_ID", "ELAPSED_TIME_S", "IS_MOVING", "LATITUDE", "LONGITUDE", "TEMPERATURE_C", "GRADE_PERCENT", "ELEVATION_M", "CADENCE_RPM", "DISTANCE_M", "HEARTRATE_BPM", "SPEED_MS", "POWER_WATTS", "EXTRACTED_TIMESTAMP", "RECORD_SOURCE")

;
    commit;