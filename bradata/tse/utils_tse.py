import bradata.connection
from bradata.utils import  _unzip

import os
import glob
import pandas as pd


def unzip_tse(result, current_path):

    if not os.path.exists(current_path):
        os.makedirs(current_path)
    filepath = os.path.join(current_path, 'temp.zip')

    with open((filepath), 'wb') as f:
        f.write(result)

    _unzip(filepath, current_path)

    os.remove(filepath)

def aggregate_tse(path, data_type, year):

    year =  year
    files = glob.glob("{}/*_{}_*.txt".format(path, year))

    headers = pd.read_csv(os.path.join(os.getcwd(), 'bradata', 'tse', 'headersTSE.csv'))

    df_list = []
    print(files)
    for filename in sorted(files):
        if data_type == 'candidatos':
            if year >= 2014:
                header = 'CONSULTA_CAND_2014'
            elif year == 2012:
                header = 'CONSULTA_CAND_2014'
            elif year <= 2010:
                header = 'CONSULTA_CAND_2010'
            df_list.append(
                pd.read_csv(filename, sep=';', names=headers[header].dropna().tolist(), encoding='latin1'))

    full_df = pd.concat(df_list)

    full_df.to_csv(os.path.join(path + '{}'.format(data_type), '{}_{}.csv'.format(data_type, year), encoding='utf-8'))

def download_headers():

    result = bradata.connection.Connection().perform_request('https://gist.github.com/JoaoCarabetta/e2bf8437007efec84c3110cb93941850',
                                                             binary = True)
    if result['status'] == 'ok':
        result = result['content']
    else:
        print('File was not dowloaded')

    unzip_tse(result, bradata.__download_dir__)
