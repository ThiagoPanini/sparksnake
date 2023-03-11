# Organização da Biblioteca

## Módulos e classes Disponíveis

De início, é importante citar que, atualmente, a biblioteca *gluesnake* conta com um módulo único chamado `manager`. Sua proposta é consolidar classes e funcionalidades capazes de abstrair grande parte da complexidade envolvendo o desenvolvimento de jobs do Glue na AWS. Na versão atual do módulo, existem duas classes Python definidas:

- `GlueJobManager`: classe responsável por gerenciar elementos fundamentais para configuração e execução de jobs do Glue na AWS
- :material-alert-decagram:{ .mdx-pulse .warning } `GlueETLManager`: classe responsável por consolidar funcionalidades de transformação de dados utilizando pyspark. Herda todas as funcionalidades da classe `GlueJobManager` e é considerada como o elemento principal de interação com os usuários.

## Exemplos Práticos

???+ tip "Uma jornada completa de consumo da biblioteca"
    Para visualizar o poder da biblioteca *gluesnake* na prática, uma **jornada completa** de consumo foi fornecida por meio de gravações e demonstrações práticas de suas funcionalidades.

    Não deixe de acessar a [página de exemplos](exemplos.md) para ter acesso a todo este rico acervo de demonstrações!
