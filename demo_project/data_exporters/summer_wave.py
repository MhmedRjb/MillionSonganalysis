from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from pandas import DataFrame
from os import path
import gspread
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_google_cloud_storage(*args, **kwargs) -> None:

    gauth = GoogleAuth()
    drive = GoogleDrive(gauth)

    file = drive.CreateFile({'title': 'metadata.yaml'})
    file.SetContentString('Hello World!')  # or file.SetContentFile('path/to/my/file.txt')
    file.Upload()

    return print('Uploaded file with ID {}'.format(file.get('id')))
