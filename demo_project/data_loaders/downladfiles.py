import urllib.request
import os
from pyspark.sql import SparkSession



if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

spark = SparkSession.builder.master(os.getenv('SPARK_MASTER_HOST', 'local')).getOrCreate()



@data_loader
def load_data(*args, **kwargs):

    unique_artists = "http://millionsongdataset.com/sites/default/files/AdditionalFiles/unique_artists.txt"
    tracks_per_year = "http://millionsongdataset.com/sites/default/files/AdditionalFiles/tracks_per_year.txt"
    unique_tracks = "http://millionsongdataset.com/sites/default/files/AdditionalFiles/unique_tracks.txt"

    urls=[unique_artists,tracks_per_year,unique_tracks]
    for url in urls:
        filename = os.path.basename(url)
        if not os.path.exists(filename):
            print(f"{filename} downladong now ")
            urllib.request.urlretrieve(url, filename)
            print(f"{filename} done ")
        else :
            print(f"{filename} is already exist ")



    return urls 


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
