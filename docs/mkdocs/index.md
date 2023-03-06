# gluesnake

## Visão Geral

A biblioteca *gluesnake* proporciona uma forma fácil, rápida e eficiente para o desenvolvimento de aplicações Spark a serem submetidas como jobs Glue na AWS. Consolidando uma série de funcionalidades envolvendo ambas as ferramentas, *gluesnake* centraliza classes, métodos e funções codificadas em [pyspark](https://spark.apache.org/docs/latest/api/python/#:~:text=PySpark%20is%20an%20interface%20for,data%20in%20a%20distributed%20environment.) que visam simplificar ao máximo a jornada de imersão em jobs Glue.

<div align="center">
    <br><img src="https://github.com/ThiagoPanini/gluesnake/blob/feature/gluesnake-module/docs/assets/imgs/logo.png?raw=true" alt="gluesnake-logo" width=200 height=200>
</div>

<div align="center">
    <i>gluesnake<br>
    Python Library</i>
</div>

<div align="center">  
  <br>
  <a href="https://pypi.org/project/gluesnake/">
    <img src="https://img.shields.io/pypi/v/gluesnake?color=purple" alt="Shield gluesnake PyPI version">
  </a>

  <a href="https://pypi.org/project/gluesnake/">
    <img src="https://img.shields.io/pypi/dm/gluesnake?color=purple" alt="Shield gluesnake PyPI downloads">
  </a>

  <a href="https://pypi.org/project/gluesnake/">
    <img src="https://img.shields.io/pypi/status/gluesnake?color=purple" alt="Shield gluesnake PyPI status">
  </a>
  
  <img src="https://img.shields.io/github/commit-activity/m/ThiagoPanini/gluesnake?color=purple" alt="Shield github commit activity">
  
  <img src="https://img.shields.io/github/last-commit/ThiagoPanini/gluesnake?color=purple" alt="Shield github last commit">

  <br>
  
  <img src="https://img.shields.io/github/actions/workflow/status/ThiagoPanini/gluesnake/ci-main.yml?label=ci" alt="Shield github CI workflow">

  <a href='https://gluesnake.readthedocs.io/pt/latest/?badge=latest'>
    <img src='https://readthedocs.org/projects/gluesnake/badge/?version=latest' alt='Documentation Status' />
  </a>

  <a href="https://codecov.io/gh/ThiagoPanini/gluesnake" > 
    <img src="https://codecov.io/gh/ThiagoPanini/gluesnake/branch/main/graph/badge.svg?token=zSdFO9jkD8"/> 
  </a>

</div>

___

## Funcionalidades

- 🤖 Simplificação de construção de aplicações Spark através de classes e métodos já codificados
- 🌟 Consolidação de funcionalidades mais comuns envolvendo processos de ETL em pyspark
- ⚙️ Abstração do todo o processo de *setup* de um *job* Glue através de uma linha de código
- 👁️‍🗨️ Aprimoramento do *observability* da aplicação através de mensagens detalhadas de log no CloudWatch
- 🛠️ Tratamento de exceções já embutidos nos métodos da biblioteca


## Instalação

A última versão da biblioteca *gluesnake* já está publicada no [PyPI](https://pypi.org/project/gluesnake/) e disponível para uso totalmente gratuito por qualquer um interessado em aprimorar a construção de suas aplicações Spark utilizando o serviço Glue. Para iniciar sua jornada de uso, basta realizar sua instalação através do seguinte comando:

```bash
pip install gluesnake
```

??? tip "Sobre ambientes virtuais Python"
    Em geral, uma boa prática relacionada a criação de novos projetos Python diz respeito à criação e uso de [ambientes virtuais](https://docs.python.org/3/library/venv.html) (ou *virtual environments*, no inglês). Criar um *venv* para cada projeto Python iniciado permite, entre outras vantagens, ter em mãos um ambiente isolado com um controle mais refinado sobre as dependências utilizadas.

    ??? example "Criando ambientes virtuais"
        Para criar um ambiente virtual Python, basta navegar até um diretório escolhido para organizar todos os *virtual envs* criados e executar o seguinte comando:

        ```bash
        python -m venv <nome_venv>
        ```

        Onde `<nome_venv>` deve ser substituído pelo nome escolhido para o ambiente virtual a ser criado. É comum ter nomes de ambientes virtuais associados à projetos (ex: `project_venv`).

    ??? example "Acessando ambientes virtuais"
        Criar um *virtual env* é apenas a primeira etapa do processo. Após criado, o ambiente precisa ser explicitamente acessado pelo usuário para garantir que todas as ações subsequentes relacionadas à instalação de bibliotecas sejam realizadas, de fato, no ambiente isolado criado.
        
        Se o sistema operacional utilizado é Windows, então use o comando abaixo para acessar o ambiente virtual Python:

        ```bash
        # Acessando ambiente virtual no Windows
        <caminho_venv>/Scripts/activate
        ```

        Em caso de uso de um sistema operacional Linux (ou Git Bash no Windows), o comando possui pequenas alterações e é dado por:

        ```bash
        # Acessando ambiente virtual no Linux
        source <caminho_venv>/Scripts/activate
        ```

        Onde `<caminho_venv>` é a referência da localização do ambiente virtual recém criado. Por exemplo, se você criou o ambiente virtual de nome *test_venv* no seu diretório de usuário, então `<caminho_venv>` pode ser substituído por `C:\Users\usuario\test_venv` no Windows ou simplesmente `~/test_venv` no Linux.
    
    Para mais informações, o [excelente artigo do blog Real Python](https://realpython.com/python-virtual-environments-a-primer/) poderá esclarecer uma série de dúvidas envolvendo a criação e o uso de ambientes virtuais Python.


## Contatos

- :fontawesome-brands-github: [@ThiagoPanini](https://github.com/ThiagoPanini)
- :fontawesome-brands-linkedin: [Thiago Panini](https://www.linkedin.com/in/thiago-panini/)
- :fontawesome-brands-hashnode: [panini-tech-lab](https://panini.hashnode.dev/)

