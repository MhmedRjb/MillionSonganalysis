if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
from oauth2client.service_account import ServiceAccountCredentials
import gspread
import pandas as pd
from demo_project.custom.sparkconnection import spark
from demo_project.custom.dataclass import datafiles
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import when, rand
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
def write_to_google_sheets(dataframe, sheet_name, credentials_file):
    # Authenticate with Google Sheets using service account credentials
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_name(credentials_file, scope)
    gc = gspread.authorize(credentials)

    # Open the Google Sheets spreadsheet by name
    sheet = gc.open(sheet_name).sheet1

    # Convert DataFrame to a format suitable for Google Sheets (list of lists)
    data = dataframe.values.tolist()

    # Update the data in the spreadsheet
    sheet.clear()  # Clear existing data
    sheet.update([dataframe.columns.values.tolist()] + data)  # Write header and data




@transformer
def transform(*args, **kwargs):
    artist_gender_df = spark.read.parquet("datafiles/unique_artists_with_gender")
    yearly_track_count_df = spark.read.parquet("datafiles/tracks_per_year")

    artist_gender_assigned_df = assign_gender(artist_gender_df)
    gender_count_df = artist_gender_assigned_df.groupBy('gender').count().orderBy('count', ascending=True)
    

    # gender_count_output_path = "output/gender_count.csv"
    # gender_count_df.write.csv(gender_count_output_path, header=True, mode="overwrite")

    write_to_google_sheets(gender_count_df, "hi", "ancient-bond-413701-80a01d0a4a8b.json")

    # Calculate gender yearly count and save to CSV
    gender_yearly_count_df = artist_gender_assigned_df.join(yearly_track_count_df, yearly_track_count_df.artistname == artist_gender_assigned_df.artistname).groupBy('gender','songyear').count()
    gender_yearly_count_output_path = "output/gender_yearly_count.csv"
    gender_yearly_count_df.write.csv(gender_yearly_count_output_path, header=True, mode="overwrite")

    # Calculate yearly track count sorted and save to CSV
    yearly_track_count_sorted_df = yearly_track_count_df.groupBy('songyear').count().orderBy('count', ascending=True)
    write_to_google_sheets(yearly_track_count_sorted_df, "hi", "ancient-bond-413701-80a01d0a4a8b.json")

    yearly_track_count_sorted_output_path = "output/yearly_track_count_sorted.csv"
    yearly_track_count_sorted_df.write.csv(yearly_track_count_sorted_output_path, header=True, mode="overwrite")
    
    return yearly_track_count_sorted_df

# pandas_df = pd.DataFrame({'Column1': [1, 23, 3], 'Column2': ['A', 'B', 'C']})
# write_to_google_sheets(pandas_df, "hi", "ancient-bond-413701-80a01d0a4a8b.json")

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
