if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
from demo_project.custom.dataclass import datafiles
import requests
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def get_gender(name):
    i=0
    url = "https://www.wikidata.org/w/api.php"
    params = {
        "action": "wbsearchentities",
        "format": "json",
        "language": "en",
        "search": name
    }
    try:
        data = requests.get(url, params=params)

        wikidataID = data.json()["search"][0]["id"]

        gender_url = f'https://www.wikidata.org/wiki/Special:EntityData/{wikidataID}.json'
        response = requests.get(gender_url)
        print(i+1)
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
    df =datafiles.unique_artists_df_par
    # pandas_df = df.toPandas()

    # df['Gender'] = df['artistname'].apply(get_gender)
    get_gender_udf = udf(get_gender, StringType())

    # Apply the UDF to the DataFrame
    df = df.withColumn('Gender', get_gender_udf(df['artistname']))
    df.write.parquet("/datafiles/unique_artists_with_gender",mode="overwrite")


    return df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
