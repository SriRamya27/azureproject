import dlt

@dlt.table # function name will be used as table name here
def dimdate_stg(): # we can read stream from table present in silver layer else we need to use autoloader again 
    df = spark.readStream.table("spotify_cata.silver.dimdate")
    return df

dlt.create_streaming_table("dimdate")

# auto cdc flow creates a dimention automatically using cdc flow
dp.create_auto_cdc_flow(
  target = "dimdate",
  source = "dimdate_stg",
  keys = ["date_key"],
  sequence_by = "date",
  stored_as_scd_type = 2,
  track_history_except_column_list = None,  
  name = None,
  once = False
) 