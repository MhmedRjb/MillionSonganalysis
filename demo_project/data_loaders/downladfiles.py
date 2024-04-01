import urllib.request
import os
from pyspark.sql import SparkSession



if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

spark = SparkSession.builder.master(os.getenv('SPARK_MASTER_HOST', 'local')).getOrCreate()

def download_file(url, filename):
    try:
        urllib.request.urlretrieve(url, filename)
        print(f"{filename} downloaded successfully")
    except Exception as e:
        print(f"Error downloading {filename}: {e}")


@data_loader
def load_data(*args, **kwargs):

    unique_artists = "http://millionsongdataset.com/sites/default/files/AdditionalFiles/unique_artists.txt"
    tracks_per_year = "http://millionsongdataset.com/sites/default/files/AdditionalFiles/tracks_per_year.txt"
    unique_tracks = "http://millionsongdataset.com/sites/default/files/AdditionalFiles/unique_tracks.txt"

    urls=[unique_artists,tracks_per_year,unique_tracks]
    for url in urls:
        filename = os.path.basename(url)
        if not os.path.exists(filename):
            print(f"{filename} downloading now")
            download_file(url, filename)
        else :
            print(f"{filename} is already exist ")
    return urls 

@test
def test_output(*args) -> None:
    files=['unique_artists.txt','tracks_per_year.txt','unique_tracks.txt']
    for filename in files :
        assert os.path.exists(filename) ==True
