import requests
import pandas as pd

URL = "https://api.alternative.me/fng/?limit=0"


def fetch_fear_greed_index():
    """ Retruns the Bitcoin fear and greed index time series. """
    response = requests.get(URL)
    if response.status_code == 200:
        data_json = response.json()
        data_df = pd.DataFrame(data_json['data'])
        data_df['date'] = pd.to_datetime(data_df['timestamp'].astype(int), unit='s')
        data_df.drop(columns=['timestamp', 'time_until_update'], inplace=True, axis=1)
        data_df.set_index('date', inplace=True)
        return data_df
    else:
        print(f"Failed to retrieve data, status code: {response.status_code}")