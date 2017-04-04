import bradata.utils
import bradata.connection

import os
import io
from zipfile import ZipFile
import pandas as pd
import glob
import yaml

import luigi

class Get_Headers(luigi.Task):

    def output(self):
        return luigi.LocalTarget(os.path.join(bradata.__download_dir__, 'tse', 'config', 'headers.csv'))

    def run(self):
        conn = bradata.connection.Connection()

        result = conn.perform_request('https://raw.githubusercontent.com/labFGV/bradata/master/bradata/tse/headersTSE.csv')

        if result['status'] == 'ok':
            result = result['content']
        else:
            print('File was not dowloaded')

        with self.output().open('w') as o_file:
            o_file.write(result)


class Get_Header_Relation(luigi.Task):
    def output(self):
        return luigi.LocalTarget(os.path.join(bradata.__download_dir__, 'tse', 'config', 'header_relation.yaml'))

    def run(self):
        conn = bradata.connection.Connection()

        result = conn.perform_request(
            'https://raw.githubusercontent.com/labFGV/bradata/master/bradata/tse/header_relation.yaml')

        if result['status'] == 'ok':
            result = result['content']
        else:
            print('File was not dowloaded')

        with self.output().open('w') as o_file:
            o_file.write(result)



class Download_Unzip(luigi.Task):
    """
    """

    year = luigi.Parameter()
    data_type = luigi.Parameter()

    def output(self):
        """
        :return: the target output for this task.
        :rtype: object (:py:class:`luigi.target.Target`)
        """
        return luigi.LocalTarget(os.path.join(bradata.__download_dir__, 'tse', 'temp', '{}_{}'.format(self.data_type, self.year)))

    def requires(self):
        """
        * :py:class:`~.Streams`
        :return: list of object (:py:class:`luigi.task.Task`)
        """
        return Get_Header_Relation()

    def run(self):
        conn = bradata.connection.Connection()

        with self.input().open('r') as input_file:
            base_url = self.select_url(self.data_type)

            url = base_url + bradata.utils._treat_inputs(self.year) + '.zip'

            result = conn.perform_request(url, binary=True)

            if result['status'] == 'ok':
                result = result['content']
            else:
                print('File was not dowloaded')

            zipfile = ZipFile(io.BytesIO(result))

            zipfile.extractall(self.output().path)

    def select_url(self, data_type):

        with open(self.input().path, 'r') as f:
            data = yaml.load(f)

        return data[data_type]['url']


class Aggregat(luigi.Task):
    """
    """

    year = luigi.Parameter()
    data_type = luigi.Parameter()

    def requires(self):
        """
        * :py:class:`~.AggregateArtists` or
        * :py:class:`~.AggregateArtistsHadoop` if :py:attr:`~/.Top10Artists.use_hadoop` is set.
        :return: object (:py:class:`luigi.task.Task`)
        """

        return {'download': Download_Unzip(data_type=self.data_type, year=self.year),
                'headers': Get_Headers(),
                'header_relation': Get_Header_Relation()}

    def output(self):
        """
        Returns the target output for this task.
        In this case, a successful execution of this task will create a file on the local filesystem.
        :return: the target output for this task.
        :rtype: object (:py:class:`luigi.target.Target`)
        """
        return luigi.LocalTarget(os.path.join(bradata.__download_dir__, 'tse', '{}_{}.csv'.format(self.data_type, self.year)))

    def run(self):

        headers = pd.read_csv(self.input()['headers'].path)
        print(self.input()['download'].path)
        files = glob.glob(self.input()['download'].path + "/*.txt".format(self.year))
        print(files)

        header = self.find_header(self.data_type, self.year)

        df_list = []
        for filename in sorted(files):
            df_list.append(
                pd.read_csv(filename, sep=';', names=headers[header].dropna().tolist(), encoding='latin1'))


        full_df = pd.concat(df_list)

        full_df.to_csv(self.output().path, index=False, encoding='utf-8')

    def find_header(self, data_type, ano):
        with open(self.input()['header_relation'].path, 'r') as f:
            data = yaml.load(f)
        a = data[data_type]['columns']

        final = 0
        for k in a.keys():
            if int(ano) >= k:
                final = k

        return str(a[final])

class Fetch(luigi.Task):

    data_types = luigi.Parameter()
    years = luigi.Parameter()

    def requires(self):

        data_types = self.string_to_list(self.data_types)
        years = self.string_to_list(self.years)

        return [Aggregat(data_type=t, year=y) for t in data_types for y in years]

    def string_to_list(self, string):
        string = string.replace("'",'').replace('[', '').replace(']','').replace(' ', '')
        return [s for s in string.split(',')]


if __name__ == "__main__":
    luigi.run()