-- activities

-- tests
-- activity_key must be unique / non-null [x]
-- average hr >= 100 & sport run/ride [x]
-- max hr <= 200 [x]
-- distance > 0 [x]
-- moving time <= elapsed time [x]
-- average hr <= max hr [x]
-- average power <= max power [x]
-- average speed <= max speed [x]
-- has_hr = True & average_hr + max_hr not null [x]
-- has_power = True & average_power + normalised_power + max_power not null [x]
-- date_key must be in date_details table [x]

-- issues
-- exclude evening run in July [x]
-- rename speed col to speed_ms [x]

-- activity streams
-- tests
-- activity_stream_key must be unique / non-null [x]
-- activity_id must be in activities [x]

