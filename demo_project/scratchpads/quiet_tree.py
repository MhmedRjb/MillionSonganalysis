import pandas as pd
import requests
import os

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

data = {'Name': ['Alberta Hunter', 'Barrington Levy', 'Papa Charlie Jackson']}
df = pd.DataFrame(data)


df['Gender'] = df['Name'].apply(get_gender)


print(df)