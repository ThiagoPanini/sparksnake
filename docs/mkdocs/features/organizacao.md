# Organização da Biblioteca

## Módulos Disponíveis

De início, é importante citar que, atualmente, a biblioteca *gluesnake* conta com um módulo único chamado `manager`. Sua proposta é consolidar classes e funcionalidades capazes de abstrair grande parte da complexidade envolvendo o desenvolvimento de jobs do Glue na AWS. Na versão atual do módulo, existem duas classes Python definidas:

- :material-alert-decagram:{ .mdx-pulse .warning } `GlueJobManager`: classe responsável por gerenciar elementos fundamentais para configuração e execução de jobs do Glue na AWS
- :material-alert-decagram:{ .mdx-pulse .warning } `GlueETLManager`: classe responsável por consolidar funcionalidades de transformação de dados utilizando pyspark

???+ note "Sobre a relação de herança entre as classes GlueETLManager e GlueJobManager"
    Introduzidos os cenários de aplicação entre ambas as classes, é extremamente importante estabeler que a classe `GlueETLManager` é a **principal porta de entrada** para os usuários. Isto pois todas as funcionalidades de tal classe herdam os atributos e métodos da classe `GlueJobManager`. Dessa forma, a classe `GlueETLManager` é tratada como o componente mais completo da biblioteca.

## Exemplos Práticos

???+ warning "Demonstrações de funcionalidades"
    Futuramente, serão proporcionados vídeos de demonstração de funcionalidades aos usuários para que estes tenham uma clara noção sobre as principais vantagens de uso da biblioteca *gluesnake*!
