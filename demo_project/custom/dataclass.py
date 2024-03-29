
from demo_project.custom.sparkconnection import spark
from pyspark.sql import types
from pyspark.sql.functions import split


class datafiles:
    unique_artists_df_par = spark.read.parquet("datafiles/unique_artists")
    tracks_per_year_df_par = spark.read.parquet("datafiles/tracks_per_year")
    unique_tracks_df_par = spark.read.parquet("datafiles/unique_tracks")

