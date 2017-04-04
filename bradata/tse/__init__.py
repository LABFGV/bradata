import os

def args(data_type=None, year=None):

    if not isinstance(data_type, list):
        data_type = [data_type]
    if not isinstance(year, list):
        year = [year]

    os.system("PYTHONPATH='.' luigi --module bradata.tse.pipeline"
              " Fetch --local-scheduler --data_type '{}' --year '{}'".format(data_type, year))


def candidatos(year=None):
    args(data_type='candidatos', year=year)

def perfil_eleitorado(year=None):
    args(data_type='perfil_eleitorado', year=year)

def bem_candidato(year=None):
    args(data_type='bem_candidato', year=year)

def legendas(year=None):
    args(data_type='legendas', year=year)

def vagas(year=None):
    args(data_type='vagas', year=year)
git
def votacao_candidato_munzona(year=None):
    args(data_type='votacao_candidato_munzona', year=year)

def votacao_partido_munzona(year=None):
    args(data_type='votacao_partido_munzona', year=year)

def votacao_secao_eleitoral(year=None):
    args(data_type='votacao_secao_eleitoral', year=year)

def vdetalhe_votacao_munzona(year=None):
    args(data_type='detalhe_votacao_munzona', year=year)



