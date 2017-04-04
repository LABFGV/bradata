import os

def args(tipo=None, year=None):

    if not isinstance(tipo, list):
        tipo = [tipo]
    if not isinstance(year, list):
        year = [year]

    os.system("PYTHONPATH='.' luigi --module bradata.tse.pipeline"
              " Fetch --local-scheduler --tipo '{}' --year '{}'".format(tipo, year))

def get_candidatos(year=None):
    args(tipo='candidatos', year=year)

def get_perfil_eleitorado(year=None):
    args(tipo='perfil_eleitorado', year=year)

def get_bem_candidato(year=None):
    args(tipo='bem_candidato', year=year)

def get_legendas(year=None):
    args(tipo='legendas', year=year)

def get_vagas(year=None):
    args(tipo='vagas', year=year)

def get_votacao_candidato_munzona(year=None):
    args(tipo='votacao_candidato_munzona', year=year)

def get_votacao_partido_munzona(year=None):
    args(tipo='votacao_partido_munzona', year=year)

def get_votacao_secao_eleitoral(year=None):
    args(tipo='votacao_secao_eleitoral', year=year)

def get_vdetalhe_votacao_munzona(year=None):
    args(tipo='detalhe_votacao_munzona', year=year)



