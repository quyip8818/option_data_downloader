import requests

def download_file(url, save_path):
    print('start downloading: ' + url)
    response = requests.get(url, stream=True)
    response.raise_for_status()
    with open(save_path, 'wb') as file:
        for chunk in response.iter_content(chunk_size=8192):
            file.write(chunk)


def get_quandl_last_day_iv_url(date_str):
    return f'https://data.nasdaq.com/api/v3/datatables/QUANTCHA/VOL.csv?date={date_str}&api_key=ka3E2qaQEpR4Ps7a8kus'
