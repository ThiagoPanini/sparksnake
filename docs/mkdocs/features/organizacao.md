# Organização da Biblioteca

Para compreender um pouco mais sobre o funcionamento da biblioteca *sparksnake* e as vantagens proporcionadas aos usuários, esta seção irá abordar detalhes sobre sua construção e a relação entre seus módulos e classes.

## Sobre a Dinâmica de Utilização

De início, é importante citar que o principal ponto de interação entre os usuários e a biblioteca *sparksnake* se dá através da classe `SparkETLManager` presente no módulo `manager`.

É através desta classe que o usuário poderá obter uma série de funcionalidades Spark pré codificadas, além de métodos específicos que podem auxiliá-lo a reduzir drasticamente a complexidade de *setup* de serviços AWS como o Glue e o EMR.

???+ quote "Mas como isso acontece na prática?"
    Essencialmente, a classe `SparkETLManager`, ao ser instanciada, exige a definição de um atributo chamado `mode`. Tal atributo tem a responsabilidade de "configurar" a classe de acordo com o serviço alvo utilizado pelo usuário na definição de sua aplicação Spark a ser implantada.

    Em termos técnicos, é através do "modo de operação" informado no atributo `mode` que a classe `SparkETLManager` é capaz de herdar funcionalidades de outras classes da biblioteca. Por exemplo, caso o usuário instancie um objeto da classe `SparkETLManager` com `mode="glue"`, então todos os atributos e métodos da classe `GlueJobManager` do módulo `glue` serão herdados para proporcionar uma **experiência personalizada** de uso do Spark dentro das especificidades do serviço Glue.

Em termos visuais, a jornada de uso da biblioteca pode ser simplificada nas três etapas abaixo:

![](https://raw.githubusercontent.com/ThiagoPanini/sparksnake/feature/lib-full-refactor/docs/assets/imgs/sparksnake-draw.png)

Para ilustrar essa dinâmica através de linhas de código (eu sei que vocês gostam), o trecho abaixo ilustra como a classe `SparkETLManager` pode ser utilizada para centralizar o uso do Spark em diferentes serviços AWS.

???+ example "Instanciando classe SparkETLManager como ponto central de desenvolvimento de aplicações Spark"

    ```python
    # Importando classe central
    from sparksnake.manager import SparkETLManager

    # Inicializando classe para implantar aplicação Spark no Glue
    spark_manager = SparkETLManager(mode="glue", **kwargs)

    # OU para implantação no EMR
    spark_manager = SparkETLManager(mode="emr", **kwargs)

    # OU até mesmo para uso sem vínculo a qualquer serviço AWS
    spark_manager = SparkETLManager(mode="spark")
    ```

    Onde `**kwargs` representam argumentos adicionais necessários de acordo com cada classe adicional herdada.

Se tudo ainda soa complexo até aqui, fique tranquilo! Os exemplos práticos e as demonstrações estão aqui para isso!

## Módulos e Classes Disponíveis

Agora que você já tem um conhecimento básico sobre o funcionamento da biblioteca *sparksnake*, a tabela abaixo visa complementar este entendimento esclarecendo todas as classes e módulos atualmente disponíveis para o usuário.

| **Classe Disponibilizada** | **Módulo** | **Descrição** |
| :-- | :-- | :-- |
| `SparkETLManager` | `manager` | Ponto central de interação com os usuários. Herda atributos e funcionalidades das demais classes de acordo com o modo de operação configurado |
| `GlueJobManager` | `glue` | Centraliza atributos e métodos específicos para serem utilizados dentro da dinâmica exigida pelo Glue como serviço |
| `EMRJobManager` | `emr` | :hammer_and_wrench: *Work in progress* |

## Exemplos Práticos

Pela própria construção da biblioteca, os exemplos práticos e demonstrações serão dividos em etapas de acordo com o serviço AWS alvo de implantação. Atualmente, estão disponíveis exemplos para os seguintes cenários:

- :material-alert-decagram:{ .mdx-pulse .warning } [Exemplo de criação de aplicações Spark para jobs do Glue](exemplos-glue.md)
