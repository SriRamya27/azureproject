import dlt

expectations ={
    "rule_1" : "user_id IS NOT NULL"
}

@dlt.table # function name will be used as table name here
@dlt.expect_all_or_drop(expectations)
def dimuser_stg(): # we can read stream from table present in silver layer else we need to use autoloader again 
    df = spark.readStream.table("spotify_cata.silver.dimuser")
    return df

# dlt.create_streaming_table("dimuser")
dlt.create_streaming_table(
    name = "dimuser",
    expect_all_or_drop = expectations
)

# auto cdc flow creates a dimention automatically using cdc flow
dp.create_auto_cdc_flow(
  target = "dimuser",
  source = "dimuser_stg",
  keys = ["user_id"],
  sequence_by = "updated_at",
  stored_as_scd_type = 2,
  track_history_except_column_list = None,  
  name = None,
  once = False
) 