if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

from demo_project.custom.sparkconnection import spark
from pyspark.sql import functions as F
from pyspark.sql.functions import when, rand
import pandas as pd
from google.cloud import bigquery
from google_auth_oauthlib import flow
from google.auth.transport.requests import Request
from pandas_gbq import to_gbq

def assign_gender(df):
    df = df.withColumn("gender", 
                       when(df["gender"] == "mostly_female", "female")
                       .otherwise(df["gender"]))
    
    df = df.withColumn("gender", 
                       when((df["gender"] == "mostly_male") | (df["gender"] == "andy"), "male")
                       .otherwise(df["gender"]))
    
    df = df.withColumn("gender", 
                       when(df["gender"] == "unknown", 
                            when(rand() > 0.3, "male").otherwise("female"))
                       .otherwise(df["gender"]))
    
    return df

def save_to_bigquery(df, project_id, dataset_id, table_id):
    # Create a BigQuery client
    client = bigquery.Client(project=project_id)

    # Convert DataFrame to Pandas DataFrame
    pandas_df = df.toPandas()

    # Upload to BigQuery
    table_ref = client.dataset(dataset_id).table(table_id)
    to_gbq(pandas_df, destination=table_ref, project_id=project_id, if_exists='replace')
@transformer
def transform(*args, **kwargs):
    artist_gender_df = spark.read.parquet("datafiles/unique_artists_with_gender")
    yearly_track_count_df = spark.read.parquet("datafiles/tracks_per_year")

    artist_gender_assigned_df = assign_gender(artist_gender_df)
    
    # Calculate gender count and save to BigQuery
    gender_count_df = artist_gender_assigned_df.groupBy('gender').count().orderBy('count', ascending=True)
    save_to_bigquery(gender_count_df, 'oval-bot-404221', 'your_dataset', 'gender_count')

    # Calculate gender yearly count and save to BigQuery
    gender_yearly_count_df = artist_gender_assigned_df.join(yearly_track_count_df, yearly_track_count_df.artistname == artist_gender_assigned_df.artistname).groupBy('gender','songyear').count()
    save_to_bigquery(gender_yearly_count_df, 'oval-bot-404221', 'your_dataset', 'gender_yearly_count')

    # Calculate yearly track count sorted and save to BigQuery
    yearly_track_count_sorted_df = yearly_track_count_df.groupBy('songyear').count().orderBy('count', ascending=True)
    save_to_bigquery(yearly_track_count_sorted_df, 'oval-bot-404221', 'your_dataset', 'yearly_track_count_sorted')
    
    return yearly_track_count_sorted_df
@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'