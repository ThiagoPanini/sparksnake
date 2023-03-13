# Jornada de Utilização: AWS Glue

Bem-vindos à página de demonstrações das funcionalidades da biblioteca *sparksnake* com seu modo de operação voltado para aplicações Spark criadas para serem utilizadas e implantadas como *jobs* Glue na AWS!

## Setup Inicial

Para proporcionar uma jornada de consumo extremamente detalhada, esta seção inicial irá comportar um passo a passo altamente didático sobre os primeiros passos relacionados ao uso das funcionalidades da bibioteca *sparksnake*. Para usuários que possuem pouca experiência no desenvolvimento de códigos Python, as demonstrações aqui consolidadas podem ajudar a esclarecer diferentes pontos e particularidades do referido pacote.

A primeira etapa dessa jornada envolve a importação da principal classe da biblioteca que funciona como um **ponto central** para todas as funcionalidades existentes. Trata-se da classe `SparkETLManager` presente no módulo `manager`.

??? example "Importando a classe SparkETLManager no script de aplicação"
    ![](https://raw.githubusercontent.com/ThiagoPanini/sparksnake/feature/lib-full-refactor/docs/assets/gifs/sparksnake-setup-import.gif)


Na sequência, como pré requisito de inicialização de um objeto da classe `SparkETLManager` para criação de *jobs* Glue na AWS, é preciso definir duas variáveis extremamente importantes:

- `argv_list`: Lista de argumentos/parâmetros utilizados no job Glue.
- `data_dict`: Dicionário contendo um mapeamento das origens do catálogo de dados a serem lidas no job Glue.

Os atributos `argv_list` e `data_dict` são exigências presentes na classe `GlueJobManager` que, por sua vez, é a classe da biblioteca *sparksnake* criada para centralizar funcionalidades específicas presentes no serviço AWS Glue. Já que a jornada aqui exemplificada considera este cenário, então a classe central `SparkETLManager` precisa receber os dois atributos obrigatórios para que a herança entre classes possa ocorrer da menria adequada.

Para que o usuário se sinta extremamente confortável nesta jornada específica de uso das funcionalidades vinculadas ao Glue, exemplos de definição dos atributos serão fornecidos logo abaixo.

??? example "Definindo variável ARGV_LIST com argumentos do job"
    ![](https://raw.githubusercontent.com/ThiagoPanini/sparksnake/feature/lib-full-refactor/docs/assets/gifs/sparksnake-setup-argvlist.gif)

    ??? tip "Sobre a lista de argumentos do job (ARGV_LIST)"
        Os elementos definidos na variável de exemplo `ARGV_LIST` necessariamente precisam ser exatamente iguais aos argumentos configurados para o job. Em outras palavras, se o usuário está utilizando alguma ferramenta de IaC (Terraform ou CloudFormation, por exemplo) e, nela, há algum tipo de bloco definindo argumentos para o job, então estes mesmos argumentos devem estar contidos na lista `ARGV_LIST`.

        Se existem argumentos ou parâmetros configurados para o job e que não estejam contidos em `ARGV_LIST`, então o Glue irá retornar um erro ao inicializar um job.

??? example "Definindo variável DATA_DICT para mapear origens de dados do job"
    ![](https://raw.githubusercontent.com/ThiagoPanini/sparksnake/feature/lib-full-refactor/docs/assets/gifs/sparksnake-setup-datadict.gif)

    ??? tip "Sobre o dicionário de mapeamento de origens de um job"
        A grande ideia de definir um dicionário de mapeamento de origens como atributo das classes do *sparksnake* é a de proporcionar um local único e centralizado para que o usuário possa coordenar todas as particularidades das origens utilizadas em seu job.

        Isto significa que todos que precisarem observar o código fonte de um job criado neste formado poderão facilmente compreender detalhes sobre as origens logo no início do script, facilitando *troubleshooting* e possíveis alterações de escopo.

        Para maiores detalhes sobre especificidades envolvendo a definição da variável `DATA_DICT`, consulte a [documentação oficial da classe `SparkETLManager`](./../mkdocstrings/SparkETLManager.md).

Por fim, um objeto da classe `SparkETLManager` pode ser instanciado.

??? example "Instanciando um objeto da classe `SparkETLManager`"
    ![](https://raw.githubusercontent.com/ThiagoPanini/sparksnake/feature/lib-full-refactor/docs/assets/gifs/sparksnake-setup-spark_manager.gif)

:star:{ .heart } Daqui em diante, demonstrações de algumas das principais funcionalidades da classe `SparkETLManager` serão fornecidas aos usuários. Tudo parte do objeto `spark_manager` devidamente instanciado e configurado de acordo com as exemplificações acima. Seus métodos aplicados proporcionam uma série de vantagens aos usuários que pretendem construir jobs Glue da melhor forma possível!

___

## Inicializando um job Glue

Após a configuração inicial do script e a criação de um objeto central, o passo subsequente da construção da aplicação envolve a inicialização de um job Glue e a obtenção de todos os seus insumos obrigatórios.

Lembra dos elementos `glueContext`, `SparkContext` e `session`? Com o método `init_job()` é possível obter todos eles como atributos da classe com uma única linha de código.

??? example "Inicializando e obtendo todos os insumos de um job Glue"
    :clapper: **Demonstração:**

    ![](https://raw.githubusercontent.com/ThiagoPanini/sparksnake/feature/lib-full-refactor/docs/assets/gifs/sparksnake-init_job.gif)

    ___

    :muscle: **Vantagens e benefícios da funcionalidade:**
    
    - [x] Obtenção de todos os insumos para execução de um job Glue
    - [x] Associação automática dos insumos como atributos da classe (ver demonstração opcional)
    - [x] Consolidação de mensagem detalhada de log com informações relevantes sobre o job

    ___

    :snake: **Código utilizado:**
    
    ```python
    # Inicializando e obtendo todos os insumos de um job Glue
    spark_manager.init_job()
    ```

    ___

    :thinking: Saiba mais em [GlueJobManager.init_job()](../../mkdocstrings/gluejobmanager/#sparksnake.manager.GlueJobManager.init_job)

    ___

    ??? tip "Opcional: analisando insumos do job obtidos como atributos da classe"
        Como mencionado, o método `init_job()` é responsável por coletar e associar os insumos de contexto Spark, contexto Glue e sessão Spark como atributos da classe instanciada. Estes três elementos formam a base de execução de funcionalidades do Glue ao longo de um job.

        ![](https://raw.githubusercontent.com/ThiagoPanini/sparksnake/feature/lib-full-refactor/docs/assets/gifs/sparksnake-opt-contexto-sessao.gif)


## Lendo Múltiplos DataFrames Spark

Em sequência à jornada de consumo, após todo o processo de *setup* e de inicialização e obtenção dos insumos do job, é chegado o momento de utilizar as informações definidas no atributo `data_dict` para realizar a leitura de objetos do tipo DataFrame Spark.

??? example "Obtendo dicionário de DataFrames Spark"
    :clapper: **Demonstração:**

    ![](https://raw.githubusercontent.com/ThiagoPanini/sparksnake/feature/lib-full-refactor/docs/assets/gifs/sparksnake-generate_dataframes_dict.gif)

    ___

    :muscle: **Vantagens e benefícios da funcionalidade:**
    
    - [x] Obtenção de múltiplos DataFrames Spark com uma única linha de código
    - [x] Melhor observability através de mensagens detalhadas de log
    - [x] Melhor organização do código a medida que o número de origens cresce
    - [x] Possibilidade de criação automática de temp views para as origens mapeadas

    ___

    :snake: **Código utilizado:**
    
    ```python
    # Lendo múltiplos DataFrames Spark de uma vez
    dfs_dict = spark_manager.generate_dataframes_dict()
    ```

    ___

    :thinking: Saiba mais em [SparkETLManager.generate_dataframes_dict()](../../mkdocstrings/SparkETLManager/#sparksnake.manager.SparkETLManager.generate_dataframes_dict)

    ___

    ??? tip "Opcional: analisando dicionário resultante e desempacotando DataFrames"
        Após a execução do método `generate_dataframes_dict()`, o usuário tem em mãos um dicionário que mapeia chaves da variável `DATA_DICT` à objetos do tipo DataFrames Spark. Para obter os DataFrames individualmente na aplicação, é preciso "desempacotar" o dicionário resultante da seguinte maneira:

        ![](https://raw.githubusercontent.com/ThiagoPanini/sparksnake/feature/lib-full-refactor/docs/assets/gifs/sparksnake-opt-generate_dataframes_dict.gif)


## Interlúdio: o que fazer agora?

Uma vez obtidos os DataFrames Spark (ou DynamicFrames Glue) na aplicação, um mar de possibilidades é aberto. Neste momento, o usuário pode aplicar seus próprios métodos de transformação, queries em SparkSQL ou qualquer outra operação que faça sentido dentro das suas próprias regras de negócio.

:material-alert-decagram:{ .mdx-pulse .warning } O *sparksnake*, como biblioteca, não possui a pretensão de encapsular toda e qualquer regra de negócio existente dentro de um *job* do Glue. Isto seria virtualmente impossível. Entretanto, uma das vantagens do *sparksnake* é possibilitar algumas funcionalidades comuns que podem ser extremamente úteis dentro da jornada de desenvolvimento de código e aplicação de regras de negócio.

## Extraindo Atributos de Data

Uma dessas funcionalidades citadas permite que os usuários enriqueçam um DataFrame Spark com uma série de atributos temporais a partir de uma coluna de data já existente. Em outras palavras, esta é uma forma fácil e eficiente de obter informações como o ano, mês, dia, quadrimestre, dia da semana e uma série de outras características com base em um atributo de data do DataFrame.

Para visualizar esta funcionalidade na prática, vamos criar uma versão simplificada do DataFrame `df_orders` com apenas alguns atributos essenciais para a demonstração.

??? tip "Preparação: criando versão simplificada do DataFrame df_orders"

    ![](https://raw.githubusercontent.com/ThiagoPanini/sparksnake/feature/lib-full-refactor/docs/assets/gifs/sparksnake-prep-df_orders_simp.gif)

Assim, o DataFrame alvo da demonstração desta funcionalidade de extração de atributos temporais contém apenas duas colunas: order_id e dt_compra. O objetivo final será obter uma série de atributos temporais com base na variável dt_compra.

??? example "Enriquecendo um DataFrame Spark com atributos temporais"
    :clapper: **Demonstração:**

    ![](https://raw.githubusercontent.com/ThiagoPanini/sparksnake/feature/lib-full-refactor/docs/assets/gifs/sparksnake-extract_date_attributes.gif)

    ___

    :muscle: **Vantagens e benefícios da funcionalidade:**
    
    - [x] Possibilidade de enriquecimento completo de DataFrame com informações de data
    - [x] Possibilidade de conversão automática de strings em campos date ou timestamp
    - [x] Aprimoramento de análise de dados com base em informações de data extraídas

    ___

    :snake: **Código utilizado:**
    
    ```python
    # Criando versão simplificada de DataFrame de pedidos
    df_orders_simp = df_orders.selectExpr(
        "order_id",
        "order_purchase_timestamp AS dt_compra"
    )

    # Extraindo atributos temporais para enriquecimento de DataFrame
    df_orders_date = spark_manager.extract_date_attributes(
        df=df_orders_simp,
        date_col="dt_compra",
        convert_string_to_date=False,
        year=True,
        month=True,
        dayofmonth=True
    )

    # Visualizando resultado
    df_orders_date.show(5)
    ```

    ___

    :thinking: Saiba mais em [SparkETLManager.extract_date_attributes()](../../mkdocstrings/SparkETLManager/#sparksnake.manager.SparkETLManager.extract_date_attributes)

    ___

    ??? tip "Opcional: extração de todos os atributos possíveis de data"
        Para trazer uma visão completa da funcionalidade em seu total poder, a demonstração abaixo utiliza todos os flags de atributos temporais existentes no método para enriquecer um DataFrame com todas as possibilidades disponíveis.

        ![](https://raw.githubusercontent.com/ThiagoPanini/sparksnake/feature/lib-full-refactor/docs/assets/gifs/sparksnake-opt-extract_date_attributes.gif)


## Extraindo Atributos Estatísticos

Uma outra funcionalidade extremamente interessante encapsulada no *sparksnake* está relacionada à extração de uma série de atributos estatísticos de uma coluna numérica presente em um DataFrame Spark. Com apenas uma chamada de método, o usuário poderá aplicar um complexo processo de agrupamento para enriquecer seus dados com atributos relevantes dentro de seu processo analítico.

Para a demonstração proposta, vamos utilizar um DataFrame alternativo que contempla dados de pagamentos realizados em pedidos online.

??? tip "Preparação: visualizando DataFrame de exemplo"
    ![](https://raw.githubusercontent.com/ThiagoPanini/sparksnake/feature/lib-full-refactor/docs/assets/gifs/sparksnake-prep-df_payments.gif)

As colunas dispostas indicam uma possibilidade analítica interessante relacionada aos valores de pagamentos realizados para cada categoria diferente. Seria interessante analisar a soma, a média, os valores mínimos e máximo, por exemplo, de pagamentos realizados em cartões de crédito e nas demais categorias. Vamos fazer isso com uma única chamada de método!

??? example "Enriquecendo DataFrame com atributos estatísticos"
    :clapper: **Demonstração:**

    ![](https://raw.githubusercontent.com/ThiagoPanini/sparksnake/feature/lib-full-refactor/docs/assets/gifs/sparksnake-extract_aggregate_statistics.gif)

    ___

    :muscle: **Vantagens e benefícios da funcionalidade:**
    
    - [x] Possibilidade de extração de múltiplos atributos estatísticos com uma única chamada
    - [x] Aprimoramento de processo analítico
    - [x] Abstração da complexidade de realizar um agrupamento complexo
    - [x] Possibilidade de agrupamento por múltiplas colunas

    ___

    :snake: **Código utilizado:**
    
    ```python
    df_payments_stats = spark_manager.extract_aggregate_statistics(
        df=df_payments,
        numeric_col="payment_value",
        group_by="payment_type",
        round_result=True,
        n_round=2,
        count=True,
        sum=True,
        mean=True,
        max=True,
        min=True,
    )

    # Visualizando resultados
    df_payments_stats.show(5)
    ```

    ___

    :thinking: Saiba mais em [SparkETLManager.extract_aggregate_statistics()](../../mkdocstrings/SparkETLManager/#sparksnake.manager.SparkETLManager.extract_aggregate_statistics)

## Dropando Partições no S3

Ao desenvolver e executar jobs Glue na AWS, uma situação que se faz sempre presente é o particionamento de tabelas no S3. Em alguns casos, é essencial evitar criar dados duplicados em partições já existentes. Dessa forma, o método `drop_partition()` possibilita uma forma de eliminar fisicamente partições no S3 antes de processos de escrita de novos dados.

Para demonstrar essa funcionalidade, uma nova aba de uma conta AWS será aberta para comprovar a existência da partição antes da execução do método e a sua eliminação posterior à execução do mesmo.

??? example "Eliminando partições físicas no S3"
    :clapper: **Demonstração:**

    ![](https://raw.githubusercontent.com/ThiagoPanini/sparksnake/feature/lib-full-refactor/docs/assets/gifs/sparksnake-drop_partition.gif)

    ___

    :muscle: **Vantagens e benefícios da funcionalidade:**
    
    - [x] Aplicação de *purge* em partição física do S3
    - [x] Possibilidade de encadeamento de drop e escrita de dados em um único fluxo
    - [x] Possibilidade de evitar duplicidade de dados ao executar um job múltiplas vezes em uma mesma janela

    ___

    :snake: **Código utilizado:**
    
    ```python
    # Definindo URI de partição
    partition_uri = "s3://some-bucket-name/some-table-name/some-partition-prefix/"

    # Eliminando fisicamente a partição do S3
    spark_manager.drop_partition(partition_uri)
    ```

    ___

    :thinking: Saiba mais em [SparkETLManager.drop_partition()](../../mkdocstrings/SparkETLManager/#sparksnake.manager.SparkETLManager.drop_partition)

## Adicionando Partições à DataFrames

Em processos ETL, é comum ter operações que geram conjuntos de dados particionados por atributos já existentes e/ou por atributos de data que referem-se ao instante temporal de execução do *job*. Para casos onde precisa-se adicionar uma ou mais colunas de partição em um DataFrame Spark já existente, o método `add_partition()` encapsula a execução do método `withColumn()` para adicionar uma coluna na coleção distribuída de dados.

??? example "Adicionando coluna de partição em um DataFrame Spark"
    :clapper: **Demonstração:**

    ![](https://raw.githubusercontent.com/ThiagoPanini/sparksnake/feature/lib-full-refactor/docs/assets/gifs/sparksnake-add_partition.gif)

    ___

    :muscle: **Vantagens e benefícios da funcionalidade:**
    
    - [x] Abstração do método de adição de coluna em um DataFrame
    - [x] Maior clareza de operações em um fluxo de ETL ao explicitar a "adição de partição" como um método
    - [x] Possibilidade de combinar os métodos `drop_partition()` e `add_partition()` em um fluxo de carga

    ___

    :snake: **Código utilizado:**
    
    ```python
    from datetime import datetime

    # Adicionando partição de anomesdia ao DataFrame
    df_payments_partitioned = spark_manager.add_partition(
        df=df_payments,
        partition_name="anomesdia",
        partition_value=int(datetime.now().strftime("%Y%m%d"))
    )

    # Visualizando resultado
    df_payments_partitioned.show(5)
    ```

    ___

    :thinking: Saiba mais em [SparkETLManager.add_partition()](../../mkdocstrings/SparkETLManager/#sparksnake.manager.SparkETLManager.add_partition)

## Reparticionando um DataFrame

Em alguns fluxos de trabalho, pensar em maneiras de otimizar o armazenamento dos conjuntos distribuídos é essencial para garantir a eficiência e promover melhores práticas de consumo dos produtos de dados gerados no processo. Aplicar métodos de reparticionamento de DataFrames Spark contribuem para uma série de fatores positivos em fluxos de ETL e, na biblioteca *sparksnake*, o método `repartition_dataframe()` foi criado para auxiliar o usuário neste processo. Basta fornecer um número alvo de partições e a própria funcionalidade irá gerenciar qual método do Spark é mais adequado para o caso (`coalesce()` o `repartition()`).

??? example "Modificando o número de partições físicas de um DataFrame"
    :clapper: **Demonstração:**

    ![](https://raw.githubusercontent.com/ThiagoPanini/sparksnake/feature/lib-full-refactor/docs/assets/gifs/sparksnake-repartition.gif)

    ___

    :muscle: **Vantagens e benefícios da funcionalidade:**
    
    - [x] Possibilidade de otimizar o processo de armazenamento de dados no sistema de armazenamento distribuído (ex: S3)
    - [x] Contribui para a redução de *small files* de uma tabela armazenada
    - [x] Contribui com um aumento de performance de consultas realizadas na tabela gerada
    - [x] Seleção automática entre os métodos `coalesce()` e `repartition()` com base no número atual coletado de partições do DataFrame

    ___

    :snake: **Código utilizado:**
    
    ```python
    # Verificando número atual de partições do DataFrame
    df_orders.rdd.getNumPartitions()

    # Diminuindo número de partições do DataFrame
    df_orders_partitioned = spark_manager.repartition_dataframe(
        df=df_orders,
        num_partitions=5
    )

    # Verificando novo número de partições
    df_orders_partitioned.rdd.getNumPartitions()
    ```
    ___

    :thinking: Saiba mais em [SparkETLManager.repartition_dataframe()](../../mkdocstrings/SparkETLManager/#sparksnake.manager.SparkETLManager.repartition_dataframe)

    ___

    ??? tip "Opcional: alerta de logs em caso de aumento no número de partições"
        O método `repartition_dataframe()` funciona da seguinte forma:

        1. Verifica-se o número atual de partições do DataFrame alvo
        2. Verifica-se o número desejado de partições passado como parâmetro
        
        * Se o número desejado for MENOR que o número atual, então o método `coalesce()` é executado
        * Caso o número desejado for MAIOR que o atual, então o método `repartition()` é executado

        E é justamente nesse segundo cenário que um alerta de log se faz presente ao usuário para que o mesmo se certifique de que esta é realmente a operação desejada. Isto pois o método `repartition()` implica em uma operação de *"full shuffle"*, podendo aumentar drasticamente o tempo de execução da aplicação Spark.

        Para maiores detalhes, consulte a seguinte [thread no Stack Overflow](https://stackoverflow.com/questions/31610971/spark-repartition-vs-coalesce) onde ambos os métodos são comparados em termos de operações *under the hood*.

        ![](https://raw.githubusercontent.com/ThiagoPanini/sparksnake/feature/lib-full-refactor/docs/assets/gifs/sparksnake-opt-repartition.gif)

## Escrevendo e Catalogando Dados

Por fim, vamos supor agora que o usuário da biblioteca tenha a missão de escrever e catalogar os resultados das transformações codificadas. No Glue, de maneira nativa, os métodos necessários a serem chamados são:

- `.getSink()`
- `.setCatalogInfo()`
- `.writeFrame()`

Na visão do usuário, seria muito fácil se um único método pudesse ser executado para abstrair e encapsular todos os procedimentos necessários para escrita de um dado no S3 e sua posterior catalogação no Data Catalog da AWS. Para isso, o método `write_data()` se faz presente!

??? example "Escrevendo dados no S3 e catalogando no Data Catalog"
    :clapper: **Demonstração:**

    - [ ] A realizar gravação

    ___

    :muscle: **Vantagens e benefícios da funcionalidade:**
    
    - [x] Abstração de diferentes métodos de escrita e catalogação em uma única chamada
    - [x] Possibilita uma maior organização da aplicação Spark desenvolvida
    - [x] Considera uma série de parametrizações para configurar especifidades na escrita dos dados

    ___

    :snake: **Código utilizado:**
    
    ```python
    # Definindo URI de tabela de saída
    s3_output_uri = "s3://some-bucket-name/some-table-name"

    spark_manager.write_data_to_catalog(
        df=df_payments_partitioned,
        s3_table_uri=s3_output_uri,
        output_database_name="ra8",
        output_table_name="tbl_payments",
        partition_name="anomesdia"   
    )
    ```
    ___

    :thinking: Saiba mais em [SparkETLManager.write_data()](../../mkdocstrings/SparkETLManager/#sparksnake.manager.SparkETLManager.write_data)

