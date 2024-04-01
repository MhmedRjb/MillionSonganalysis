if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
from oauth2client.service_account import ServiceAccountCredentials
import gspread
import pandas as pd
from demo_project.custom.sparkconnection import spark
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import when, rand
# becuse i have a problem in resources i use this function to complete the project if you know a methode please help
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

 
def write_to_google_sheets(df, sheet_name, credentials_file,Name):
    # Authenticate with Google Sheets using service account credentials
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_name(credentials_file, scope)
    gc = gspread.authorize(credentials)
    gender_count_df=df
    header = gender_count_df.columns
    data = [list(row) for row in gender_count_df.collect()]
    # Open the Google Sheets spreadsheet by name
    spreadsheet = gc.open(sheet_name)
    sheet = spreadsheet.worksheet(Name)
    sheet.clear()  # Clear existing data
    sheet.update([header]+data)  # Write header and data





@transformer
def transform(*args, **kwargs):
    artist_gender_df = spark.read.parquet("datafiles/unique_artists_with_gender")
    yearly_track_count_df = spark.read.parquet("datafiles/tracks_per_year")

    artist_gender_assigned_df = assign_gender(artist_gender_df)
    gender_count_df = artist_gender_assigned_df.groupBy('gender').count().orderBy('count', ascending=True)
    write_to_google_sheets(gender_count_df, "MillionSongsanalysis", "Serviceaccounts.json","gender_count")



    gender_yearly_count_df = artist_gender_assigned_df.join(yearly_track_count_df, yearly_track_count_df.artistname == artist_gender_assigned_df.artistname).groupBy('gender','songyear').count()
    write_to_google_sheets(gender_yearly_count_df, "MillionSongsanalysis", "Serviceaccounts.json","gender_yearly")

    yearly_track_count_sorted_df = yearly_track_count_df.groupBy('songyear').count().orderBy('count', ascending=True)
    write_to_google_sheets(yearly_track_count_sorted_df, "MillionSongsanalysis", "Serviceaccounts.json","yearly_track_count_sorted")

    
    return "done"

# pandas_df = pd.DataFrame({'Column1': [1, 23, 3], 'Column2': ['A', 'B', 'C']})
# write_to_google_sheets(pandas_df, "hi", "ancient-bond-413701-80a01d0a4a8b.json")

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
