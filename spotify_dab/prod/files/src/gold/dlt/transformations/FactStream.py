import dlt

@dlt.table # function name will be used as table name here
def factstream_stg(): # we can read stream from table present in silver layer else we need to use autoloader again 
    df = spark.readStream.table("spotify_cata.silver.factstream")
    return df

dlt.create_streaming_table("factstream")

# auto cdc flow creates a dimention automatically using cdc flow
dp.create_auto_cdc_flow(
  target = "factstream",
  source = "factstream_stg",
  keys = ["stream_id"],
  sequence_by = "stream_timestamp",
  stored_as_scd_type = 1,
  track_history_except_column_list = None,  
  name = None,
  once = False
) 