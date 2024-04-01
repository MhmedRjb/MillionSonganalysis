if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
import requests
from pyspark.sql import SparkSession, functions as F
import gender_guesser.detector as gender
from pyspark.sql.functions import udf

import os

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from demo_project.custom.sparkconnection import spark
import time

def get_gender(name):
    url = "https://www.wikidata.org/w/api.php"
    params = {
        "action": "wbsearchentities",
        "format": "json",
        "language": "en",
        "search": name}
    try:
        data = requests.get(url, params=params)

        wikidataID = data.json()["search"][0]["id"]

        gender_url = f'https://www.wikidata.org/wiki/Special:EntityData/{wikidataID}.json'
        response = requests.get(gender_url)
        gender_data = response.json()
        try:
            gender = gender_data['entities'][wikidataID]['claims']['P21'][0]['mainsnak']['datavalue']['value']['id']
            return gender
        except KeyError:
            return "Error: Invalid JSON structure or key path not found."
    except:
        return "null"

@transformer
def transform(*args, **kwargs):
    df=spark.read.parquet("datafiles/unique_artists")
    if os.path.exists('datafiles/unique_artists_with_gender'):
        pass 

    else:
        udf_gender = F.udf(get_gender_ai, F.StringType())  # Create reusable UDF

        transformed_df = df.withColumn("gender", udf_gender(F.col("artistname")))
        transformed_df.repartition(6).write.parquet("datafiles/unique_artists_with_gender")
    #  i used AI to save time and resources i tried to use the api but every request take 1 sec and i did't find a way to optimize that ,help is welocme

    return "done"
@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
