# Organização dos Módulos e Classes

## Módulos Disponíveis

Para compreender detalhes sobre as funcionalidades da biblitoeca *gluesnake*, é preciso ter uma noção básica sobre como os componentes estão organizados dentro da estrutura proposta.

De forma objetiva e direta, atualmente existe um único módulo na biblioteca chamado `manager`. 

A CONSTRUIR

Gerenciamento de operações no serviço AWS Glue.

Módulo criado para consolidar classes, métodos, atributos, funções e implementações no geral que possam facilitar a construção de jobs Glue utilizando a linguagem Python como principal ferramenta.

Os usuários consumidores das funcionalidades deste módulo poderão importar as classes disponibilizadas e utilizar métodos específicos em prol do aprimoramento de suas respectivas jornadas de construção de aplicações Spark como jobs do Glue na AWS.

??? tip "Principais classes presentes no módulo"
    O módulo manager.py é formado por duas classes Python essenciais que comportam toda a lógica da biblioteca. São elas:

    - GlueJobManager: classe responsável por gerenciar elementos
    fundamentais para configuração e execução de jobs do Glue na AWS
    - GlueETLManager: classe responsável por consolidar funcionalidades
    de transformação de dados utilizando pyspark
