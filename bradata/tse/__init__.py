import os
import bradata
import shutil
import pandas as pd
import bradata

def call_pipeline(data_type=None, year=None, *args, **kwargs):

    # transform in list

    # check if is a valid year

    if year % 2 != 0:
        raise KeyError ('Years must be even')
    # call pipeline
    os.system("PYTHONPATH='.' luigi --module bradata.tse.pipeline"
              " Aggregat --local-scheduler   --logging-conf-file bradata/tse/luigi.cfg "
              "--data-type {} --year {}".format(data_type, year))

    # erase temp files if completed

    if os.path.isfile(os.path.join(bradata.__download_dir__, 'tse', '{}_{}.csv'.format(data_type, year))):
        try:
            shutil.rmtree(os.path.join(bradata.__download_dir__, 'tse', 'temp', '{}_{}'.format(data_type, year)))
        except:
            pass


def candidatos(year=None, *args, **kwargs):
    """
    Outputs a csv file with aggregate information from all brazilian states about Candidatos.

    It is working for the years 1994, 1996, 1998, 2000, 2002, 2004, 2006, 2008, 2010, 2012, 2014, 2016

    Args:
        year: int

    Returns: CSV file
    """
    call_pipeline(data_type='candidatos', year=year)

def perfil_eleitorado(year=None, *args, **kwargs):
    """
    Outputs a csv file with aggregate information from all brazilian states about Perfil Eleitorado.

    It is working for the years 1994, 1996, 1998, 2000, 2002, 2004, 2006, 2008, 2010, 2012, 2014, 2016

    Args:
        year: int

    Returns: CSV file
    """
    call_pipeline(data_type='perfil_eleitorado', year=year)

def bem_candidato(year=None, *args, **kwargs):
    """
    Outputs a csv file with aggregate information from all brazilian states about Bem Candidatos.

    It is working for the years 2006, 2008, 2010, 2012, 2014, 2016

    Args:
        year: int

    Returns: CSV file
    """
    call_pipeline(data_type='bem_candidato', year=year)

def legendas(year=None, *args, **kwargs):
    """
    Outputs a csv file with aggregate information from all brazilian states about Legendas.

    It is working for the years 1994, 1996, 1998, 2000, 2002, 2004, 2006, 2008, 2010, 2012, 2014, 2016

    Args:
        year: int

    Returns: CSV file
    """
    call_pipeline(data_type='legendas', year=year)


def vagas(year=None, *args, **kwargs):
    """
    Outputs a csv file with aggregate information from all brazilian states about Vagas.

    It is working for the years 1994, 1996, 1998, 2000, 2002, 2004, 2006, 2008, 2010, 2012, 2014, 2016

    Args:
        year: int

    Returns: CSV file
    """
    call_pipeline(data_type='vagas', year=year, *args, **kwargs)


def votacao_candidato_munzona(year=None, *args, **kwargs):
    """
    Outputs a csv file with aggregate information from all brazilian states about Votacao Candidato Municipio Zona.

    It is working for the years 1994, 1996, 1998, 2000, 2002, 2004, 2006, 2008, 2010, 2012, 2014, 2016

    Args:
        year: int

    Returns: CSV file
    """
    call_pipeline(data_type='votacao_candidato_munzona', year=year)

def votacao_partido_munzona(year=None, *args, **kwargs):
    """
    Outputs a csv file with aggregate information from all brazilian states about Votacao Partido Municipio Zona.

    It is working for the years 1994, 1996, 1998, 2000, 2002, 2004, 2006, 2008, 2010, 2012, 2014, 2016

    Args:
        year: int

    Returns: CSV file
    """
    call_pipeline(data_type='votacao_partido_munzona', year=year)

def votacao_secao_eleitoral(year=None, *args, **kwargs):
    call_pipeline(data_type='votacao_secao_eleitoral', year=year)

def detalhe_votacao_munzona(year=None, *args, **kwargs):
    """
    Outputs a csv file with aggregate information from all brazilian states about Detalhe Votacao Municipio Zona.

    It is working for the years 1994, 1996, 1998, 2000, 2002, 2004, 2006, 2008, 2010, 2012, 2014, 2016

    Args:
        year: int

    Returns: CSV file
    """
    call_pipeline(data_type='detalhe_votacao_munzona', year=year)

def reset():
    shutil.rmtree(os.path.join(bradata.__download_dir__, 'tse', 'config'))
    print(os.path.join(bradata.__download_dir__, 'tse', 'config'), 'Removed')





