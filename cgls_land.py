import concurrent
import os.path

import numpy as np
import pandas as pd
import requests
import re

from tqdm.auto import tqdm
from concurrent.futures import ThreadPoolExecutor


class Session(object):

    def __init__(self, usr, psw):

        self.session = requests.Session()
        if usr == '' or psw == '':
            raise Exception('User and password are mandatory for downloading. Registration available at: '
                            'https://land.copernicus.vgt.vito.be/PDF/portal/Application.html#Home (top right)')

        self.session.auth = (usr, psw)
        self.url = 'https://land.copernicus.vgt.vito.be/manifest/'

        try:
            connection = self.session.get(self.url)
            connection.raise_for_status()

        except requests.exceptions.HTTPError as err:
            print(err)
            return

        self.products = None

    @property
    def collections(self):
        if self.products is None:
            _ = self.list_collections()
        print(*self.products.tolist(), sep='\n')

    def list_collections(self) -> list:

        try:
            manifest_response = self.session.get(self.url, allow_redirects=True)
            manifest_response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            print(err)
            return None

        self.products = pd.read_html(manifest_response.text, skiprows=2)[0]['Parent Directory'].dropna()
        self.products = self.products.apply(lambda x: x.replace('/', ''))

        return self.products.tolist()

    def load_collection(self, product_name: str):

        if self.products is None:
            _ = self.list_collections()

        if self.products.str.contains(product_name, regex=False).any():
            product = product_name
        else:
            raise Exception('Product not in the list')

        url = f'{self.url}{product}/'

        try:
            col_manifest_response = self.session.get(url, allow_redirects=True)
            col_manifest_response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            print(err)
            return

        int_product_list = pd.read_html(col_manifest_response.text, skiprows=2)[0]['Parent Directory'].dropna(
         ).values[-1]
        purl = f'{url}{int_product_list}'

        try:
            specific_manifest = self.session.get(purl, stream=True)
        except requests.exceptions.HTTPError as err:
            print(err)
            return

        rows = specific_manifest.text.split('\n')
        observation_table = pd.DataFrame()

        for n_index, line in enumerate(rows[:-1]):
            r = re.search(r"\d\d\d\d/\d\d/\d\d", line)
            exploded = line.split('/')
            int_path = os.path.join(*exploded[-8:-1])
            name = exploded[-2]
            name_components = name.split('_')
            sensor = name_components[-2]
            version = name_components[-1][-5:]

            if '-RT' in name:
                RT = name_components[0][-3:]
                observation_table = pd.concat(
                    [observation_table, pd.DataFrame([[name, int_path, exploded[-1], line, sensor, version, RT,
                                                       pd.to_datetime(r[0], format="%Y/%m/%d")]],
                                                     columns=['name', 'int_path', 'file_name', 'url', 'sensor',
                                                              'version', 'rt', 'date'],
                                                     index=[n_index]
                                                     )
                     ])

            else:
                observation_table = pd.concat(
                    [observation_table, pd.DataFrame([[name, int_path, exploded[-1], line, sensor, version,
                                                       pd.to_datetime(r[0], format="%Y/%m/%d")]],
                                                     columns=['name', 'int_path', 'file_name', 'url', 'sensor',
                                                              'version', 'date'],
                                                     index=[n_index]
                                                     )
                     ])

            if '-RT' in name:
                observation_table = observation_table.sort_values(by=['date', 'rt'], ascending=True).reset_index(
                    drop=True)
            else:
                observation_table = observation_table.sort_values(by=['date'], ascending=True).reset_index(drop=True)

        return Collection(product_name, observation_table, self.session)


class Collection:

    def __init__(self, name, observation_table, session):
        self.session = session
        self.name = name
        self.observation_table = observation_table
        self.sensors = None
        self.start_date = None
        self.end_date = None
        self.alg_version_list = None
        self.rt = None
        self.path = None

    def get_date_range(self):
        self.start_date = self.observation_table['date'].head(1).item()
        self.end_date = self.observation_table['date'].tail(1).item()

        return np.array([str(self.start_date.date()), str(self.end_date.date())])

    def get_alg_version_list(self):
        self.alg_version_list = self.observation_table['version'].unique()
        return self.alg_version_list

    def get_rt(self):
        if 'rt' in self.observation_table.columns:
            self.rt = self.observation_table['rt'].unique()
            return self.rt
        else:
            return None

    def get_sensors(self):
        self.sensors = self.observation_table['sensor'].unique()
        return self.sensors

    @property
    def infos(self):
        if self.sensors is None:
            _ = self.get_sensors()
        if self.start_date is None or self.end_date is None:
            _ = self.get_date_range()
        if self.alg_version_list is None:
            _ = self.get_alg_version_list()
        if self.rt is None:
            if 'rt' in self.observation_table.columns:
                _ = self.get_rt()

        print(fr'''
Product name:      {self.name} 
Sensor           : {self.sensors}
Valid time period: [{self.start_date.date()}:{self.end_date.date()}]
Algoritms        : {self.alg_version_list}
RT list          : {self.rt}''')

    def download(self, date=None, path=None, **kwargs):

        # RT
        if 'rt' in self.observation_table.columns:
            if 'rt' in kwargs:
                rt = kwargs['rt']
                self.observation_table = self.observation_table[self.observation_table.RT == rt]
            else:
                self.observation_table.drop_duplicates(subset=['date'], keep='last', inplace=True)

        # date
        if self.start_date is None or self.end_date is None:
            _ = self.get_date_range()

        self.observation_table.set_index(['date'], inplace=True, drop=True)

        download_list = []
        if date is None:
            link, filename, int_path = self.observation_table.tail(1).item()[['url', 'file_name', 'int_path']]
            download_list.append([link, filename, int_path])
        elif isinstance(date, str):
            req_date = pd.to_datetime(date)

            if not self.start_date <= req_date <= self.end_date:
                raise Exception('Date are out of the valid range')

            i = self.observation_table.index.get_indexer([req_date], method='ffill')[0]
            download_list.append(self.observation_table.iloc[i][['url', 'file_name', 'int_path']].values.tolist())

        elif isinstance(date, list):
            for n_date in date:
                req_date = pd.to_datetime(n_date)

                if not self.start_date <= req_date <= self.end_date:
                    raise Exception('Date are out of the valid range')

                i = self.observation_table.index.get_indexer([req_date], method='ffill')[0]
                download_list.append(self.observation_table.iloc[i][['url', 'file_name', 'int_path']].values.tolist())
        elif isinstance(date, slice):

            assert (self.start_date <= date.start <= self.end_date) or \
                   (self.start_date <= date.stop <= self.end_date), 'Date are out of the valid range'
            assert date.start <= date.stop, 'End date is before the start date'
            assert date.start != date.stop, 'Dates coincide, please use only one date'

            i_start = self.observation_table.index.get_indexer([date.start], method='ffill')[0]
            i_end   = self.observation_table.index.get_indexer([date.stop],  method='ffill')[0]

            download_list = self.observation_table.iloc[i_start:i_end+1][['url', 'file_name',
                                                                         'int_path']].values.tolist()

        # path
        self.path = path
        if self.path is not None:
            if not os.path.isabs(self.path):
                self.path = os.path.abspath(self.path)
        else:
            self.path = os.path.join(os.path.curdir, 'data', download_list[0][2])  # int_path

        if not os.path.isdir(self.path):
            os.makedirs(self.path, mode=0o777, exist_ok=True)

        downloaded_list = []

        dl_tasks = {}
        with ThreadPoolExecutor(max_workers=4) as executor:
            for pid, values in enumerate(download_list):
                url, file_name, int_path = values
                future = executor.submit(self._get_file, url, file_name)
                dl_tasks[future] = pid

            for task in concurrent.futures.as_completed(list(dl_tasks)):
                try:
                    file_name_d = task.result()
                    downloaded_list.append(file_name_d)
                except Exception as exc:
                    print('%r generated an exception: %s' % (url, exc))

        if not downloaded_list:
            raise Exception('0 files downloaded')

        return downloaded_list

    def _get_file(self, url, file_name):

        d_path = os.path.join(self.path, file_name)

        # filname
        try:
            r = self.session.get(url, stream=True)  # link
            r.raise_for_status()
        except requests.exceptions.HTTPError as err:
            print(err)
            return

        total_size = int(r.headers.get('content-length', 0))
        block_size = 1024  # 1 Kibibyte
        downloaded_bytes = 0

        if os.path.isfile(d_path) and os.path.getsize(d_path) == total_size:
            print(f'Skipping {file_name} as already available')
            return d_path

        with tqdm(total=total_size, unit='iB', unit_scale=True, leave=False, desc=file_name) as progress:
            with open(d_path, 'wb') as f:
                for chunk in r.iter_content(block_size):
                    f.write(chunk)
                    progress.update(len(chunk))
                    downloaded_bytes += len(chunk)

        if total_size != downloaded_bytes:
            return Exception((file_name, "Size incompatible with the original"))

        return d_path


