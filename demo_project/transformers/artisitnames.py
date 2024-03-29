if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


from demo_project.custom.sparkconnection import spark
from pyspark.sql import types
from pyspark.sql.functions import split
from demo_project.custom.dataclass import datafiles

def get_gender(name):
    url = "https://www.wikidata.org/w/api.php"
    params = {
        "action": "wbsearchentities",
        "format": "json",
        "language": "en",
        "search": name
    }
    try:
        data = requests.get(url, params=params)
        gender_id = data.json()["search"][0]["id"]
        gender_url = f'https://www.wikidata.org/wiki/Special:EntityData/{gender_id}.json'
        response = requests.get(gender_url)
        gender_data = response.json()
        try:
            gender = gender_data['entities'][gender_id]['claims']['P21'][0]['mainsnak']['datavalue']['value']['id']
            return gender
        except KeyError:
            return "Error: Invalid JSON structure or key path not found."
    except:
        return "Error: Invalid Input or API request failed."

@transformer
def transform(*args, **kwargs):
    # unique_artists_df =datafiles.unique_artists_df_par
    # LIMITED=datafiles.unique_artists_df_par.limit(10)
    # LIMITED['Gender'] = LIMITED['artistname'].apply(get_gender)
    artist_names = datafiles.unique_artists_df_par.select('artistname').distinct().limit(10).rdd.flatMap(lambda x: x).collect()

# Get the genders
    genders = {name: get_gender(name) for name in artist_names}

    # Create a DataFrame from the genders dictionary
    genders_df = spark.createDataFrame(list(genders.items()), ["artistname", "Gender"])

    # Join the genders DataFrame with the original DataFrame
    result_df = datafiles.unique_artists_df_par.join(genders_df, on='artistname', how='left')

    # Show the DataFrame
    result_df.show()


    return artist_names


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
