import dlt

@dlt.table # function name will be used as table name here
def dimtrack_stg(): # we can read stream from table present in silver layer else we need to use autoloader again 
    df = spark.readStream.table("spotify_cata.silver.dimtrack")
    return df

dlt.create_streaming_table("dimtrack")

# auto cdc flow creates a dimention automatically using cdc flow
dp.create_auto_cdc_flow(
  target = "dimtrack",
  source = "dimtrack_stg",
  keys = ["track_id"],
  sequence_by = "updated_at",
  stored_as_scd_type = 2,
  track_history_except_column_list = None,  
  name = None,
  once = False
) 