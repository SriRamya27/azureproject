# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

import os
import sys

project_pth = os.path.join(os.getcwd(), "..", "..") # we always have to add project path to system path in order to import variables from a file present in a different folder
sys.path.append(project_pth)

# COMMAND ----------

project_pth

# COMMAND ----------

from utils.transformations import reusable

# COMMAND ----------

# MAGIC %md
# MAGIC ### DimUser

# COMMAND ----------

# MAGIC %md
# MAGIC ### **AUTOLOADER**

# COMMAND ----------

df_user = spark.readStream.format("CloudFiles")\
                .option("cloudFiles.format", "parquet")\
                .option("cloudFiles.schemaLocation","abfss://silver@srpspotifyproject.dfs.core.windows.net/DimUser/checkpoint")\
                .load("abfss://bronze@srpspotifyproject.dfs.core.windows.net/DimUser")

# COMMAND ----------

display(df_user)

# COMMAND ----------

# convert the user_name to upper case
df_user = df_user.withColumn("user_name", upper(col("user_name"))) # use withColumn when you want to update/modify/add a col
display(df_user) # _rescued_data is a col by default with autoloader and new cols will be added here in the rescued data col when the mode is given as 'rescue' in the schema evolution mode in this way: .option("schemaEvolutionMode", "rescue")

# COMMAND ----------

#using our utility function to drop columns
df_user_obj = reusable()

df_user = df_user_obj.dropColumns(df_user, ['_rescued_data'])
display(df_user)

# COMMAND ----------

df_user = df_user.dropDuplicates(['user_id'])
display(df_user)

# COMMAND ----------

 #.trigger(once=True) will run the query exactly once, processes all available data upto that point and then stops
df_user.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation", "abfss://silver@srpspotifyproject.dfs.core.windows.net/DimUser/checkpoint")\
    .trigger(once=True)\
    .option("path", "abfss://silver@srpspotifyproject.dfs.core.windows.net/DimUser/data")\
    .toTable("spotify_cata.silver.DimUser")
    #data in delta format has lot of partitions due to streaming as it treating data in form of micro-batches

# COMMAND ----------

# MAGIC %md
# MAGIC ### DimArtist

# COMMAND ----------

df_art = spark.readStream.format("CloudFiles")\
                .option("cloudFiles.format", "parquet")\
                .option("cloudFiles.schemaLocation","abfss://silver@srpspotifyproject.dfs.core.windows.net/DimArtist/checkpoint")\
                .load("abfss://bronze@srpspotifyproject.dfs.core.windows.net/DimArtist")

# COMMAND ----------

display(df_art)

# COMMAND ----------

df_art_obj = reusable()

df_art = df_art_obj.dropColumns(df_art, ['_rescued_data'])
df_art = df_art.dropDuplicates(['artist_id'])
display(df_art)

# COMMAND ----------

 df_art.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation", "abfss://silver@srpspotifyproject.dfs.core.windows.net/DimArtist/checkpoint")\
    .trigger(once=True)\
    .option("path", "abfss://silver@srpspotifyproject.dfs.core.windows.net/DimArtist/data")\
    .toTable("spotify_cata.silver.DimArtist")

# COMMAND ----------

# MAGIC %md
# MAGIC ### DimTrack

# COMMAND ----------

df_track = spark.readStream.format("CloudFiles")\
                .option("cloudFiles.format", "parquet")\
                .option("cloudFiles.schemaLocation","abfss://silver@srpspotifyproject.dfs.core.windows.net/DimTrack/checkpoint")\
                .load("abfss://bronze@srpspotifyproject.dfs.core.windows.net/DimTrack")

# COMMAND ----------

display(df_track)

# COMMAND ----------

# categorize col duration_sec into < 150s (low), 150-300s (medium), >300s (high)
df_track = df_track.withColumn("durationFlag",when(col("duration_sec")<150, "low")\
                                            .when(col("duration_sec")<300, "medium")\
                                            .otherwise("high"))
display(df_track)

# COMMAND ----------

# replace the hypen present in track_name col with space
df_track = df_track.withColumn("track_name", regexp_replace(col("track_name"),"-"," "))
display(df_track)

# COMMAND ----------

df_track_obj = reusable()
df_track = df_track_obj.dropColumns(df_track,['_rescued_data'])

#OR
#df_track = reusable.dropColumns(df_track,['_rescued_data'])  #class_name().function_name()

display(df_track)

# COMMAND ----------

 df_track.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation", "abfss://silver@srpspotifyproject.dfs.core.windows.net/DimTrack/checkpoint")\
    .trigger(once=True)\
    .option("path", "abfss://silver@srpspotifyproject.dfs.core.windows.net/DimTrack/data")\
    .toTable("spotify_cata.silver.DimTrack")

# COMMAND ----------

# MAGIC %md
# MAGIC ### DimDate

# COMMAND ----------

df_date = spark.readStream.format("CloudFiles")\
                .option("cloudFiles.format", "parquet")\
                .option("cloudFiles.schemaLocation","abfss://silver@srpspotifyproject.dfs.core.windows.net/DimDate/checkpoint")\
                .load("abfss://bronze@srpspotifyproject.dfs.core.windows.net/DimDate")

# COMMAND ----------

display(df_date)

# COMMAND ----------

df_date = reusable().dropColumns(df_date,['_rescued_data'])
display(df_date)

# COMMAND ----------

 df_date.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation", "abfss://silver@srpspotifyproject.dfs.core.windows.net/DimDate/checkpoint")\
    .trigger(once=True)\
    .option("path", "abfss://silver@srpspotifyproject.dfs.core.windows.net/DimDate/data")\
    .toTable("spotify_cata.silver.DimDate")

# COMMAND ----------

# MAGIC %md
# MAGIC ### FactStream

# COMMAND ----------

df_fact = spark.readStream.format("CloudFiles")\
                .option("cloudFiles.format", "parquet")\
                .option("cloudFiles.schemaLocation","abfss://silver@srpspotifyproject.dfs.core.windows.net/FactStream/checkpoint")\
                .load("abfss://bronze@srpspotifyproject.dfs.core.windows.net/FactStream")

# COMMAND ----------

display(df_fact)

# COMMAND ----------

df_fact = reusable().dropColumns(df_fact, ['_rescued_data'])

# COMMAND ----------

 df_fact.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation", "abfss://silver@srpspotifyproject.dfs.core.windows.net/FactStream/checkpoint")\
    .trigger(once=True)\
    .option("path", "abfss://silver@srpspotifyproject.dfs.core.windows.net/FactStream/data")\
    .toTable("spotify_cata.silver.FactStream")

# COMMAND ----------

