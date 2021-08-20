from google.oauth2 import service_account
from search.search_config import search
import os


def main(data: search):
    credentials = service_account.Credentials.from_service_account_file(
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'])
    print(credentials)
