if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
from demo_project.custom.sparkconnection import spark
from pyspark.sql import types
from pyspark.sql.functions import split


def process_file(file_name, columns, output_dir):
    df = spark.read.text(file_name).select(
        split('value', '<SEP>').getItem(0).alias(columns[0]),
        split('value', '<SEP>').getItem(1).alias(columns[1]),
        split('value', '<SEP>').getItem(2).alias(columns[2]),
        split('value', '<SEP>').getItem(3).alias(columns[3])
    )
    try:
        df.repartition(3).write.parquet(output_dir)
    except:
        print(f"File already exists: {output_dir}")

    return df


@transformer
def transform(*args, **kwargs):


    unique_artists_df = process_file("unique_artists.txt", ['artistid', 'artistmbid', 'trackid', 'artistname'], "datafiles/unique_artists")
    tracks_per_year_df = process_file("tracks_per_year.txt", ['songyear', 'uniqueID', 'artistname', 'Songname'], "datafiles/tracks_per_year")
    unique_tracks_df = process_file("unique_tracks.txt", ['trackid', 'songid', 'artistname', 'songtitle'], "datafiles/unique_tracks")


    return unique_artists_df

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
