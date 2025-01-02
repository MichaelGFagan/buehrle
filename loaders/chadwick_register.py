import os
import string
from urllib.request import urlretrieve
from urllib.error import HTTPError

SUFFIXES = list(range(10)) + list(string.ascii_lowercase)
PATH = os.path.dirname(__file__).replace('loaders', 'data/chadwick/register')
os.makedirs(PATH, exist_ok=True)


def chadwick_register():
    for suffix in SUFFIXES:
        filename = f'people-{suffix}.csv'
        url = f'https://raw.githubusercontent.com/chadwickbureau/register/master/data/{filename}'
        filepath = f'{PATH}/{filename}'
        print(f'Downloading {url} to {filepath}')
        try:
            urlretrieve(url, filepath)
        except HTTPError:
            print(f'Failed to download {filename}. File does not exist.')
            break

if __name__ == '__main__':
    chadwick_register()