# Usage Journey: Enhancing Glue Jobs Development with sparksnake

Welcome to the *sparksnake* library feature demos page with its operation mode focused on Spark applications designed to be used and deployed as Glue jobs on AWS! Fasten your seat belts and let's go code!

## Initial Setup

To provide an extremely detailed consumption journey, this initial section will include a highly didactic step by step on using the *sparksnake* library for the first time ever. For users who have little to none experience developing Python code, this is a really nice beginning section.

The first step in this journey goes through importing the library's main class that acts as a central point for all existing functionality. This is the class `SparkETLManager` present in the module `manager`.

??? example "Importando a classe SparkETLManager no script de aplicação"
    ![](https://raw.githubusercontent.com/ThiagoPanini/sparksnake/main/docs/assets/gifs/sparksnake-setup-import.gif)


Next, as a prerequisite for initializing a class object for creating Glue jobs on AWS, the user must define two extremely important attributes:

- `argv_list`: List of arguments/parameters used in the Glue job.
- `data_dict`: Dictionary containing a mapping of the sources on the data catalog to be read in the Glue job.

The attributes `argv_list` and `data_dict` are requirements present in the class `GlueJobManager`, which in turn is the *sparksnake* library class created to centralize specific functionality present in the AWS Glue service. Since the journey exemplified here considers this scenario, then the central class `SparkETLManager` needs to receive the two mandatory attributes so that inheritance between classes can occur on the proper way.

In order for the user to feel extremely comfortable in this specific journey of using glue-linked functionality, examples of defining the attributes will be provided below.


??? example "Defining variable ARGV_LIST containing job arguments"
    ![](https://raw.githubusercontent.com/ThiagoPanini/sparksnake/main/docs/assets/gifs/sparksnake-setup-argvlist.gif)

    ??? tip "About the job argument list"
        The elements defined in the `ARGV_LIST` variable provided as an example necessarily need to be exactly the same as the arguments configured for the job. In other words, if the user is using some IaC tool (Terraform or CloudFormation, for example) and in its definition the user declare some job parameters, then these same arguments must be contained in the `ARGV_LIST` variable.
        
        If arguments or parameters are configured for the job (directly on the console or using an IaC tool) and they are not contained in the `ARGV_LIST` variable, then Glue will return an error when initializing a job.


??? example "Defining DATA_DICT variable to map job data sources"
    ![](https://raw.githubusercontent.com/ThiagoPanini/sparksnake/main/docs/assets/gifs/sparksnake-setup-datadict.gif)

    ??? tip "About a job's source mapping dictionary"
        The great idea about defining a source mapping dictionary as an attribute of *sparksnake* classes is to provide a single, centralized location so that users can coordinate all the particularities of the sources used in their job using one variable.

        This means that anyone who needs to observe the source code of a job created in this form can easily understand details about the sources early in the script, making troubleshooting and possible scope changes easier.
        
        For more details on specificities involving variable definition, see the [official documentation of the `SparkETLManager` class](./../mkdocstrings/SparkETLManager.md).


Finally, an object of the `SparkETLManager` class can be obtained.

??? example "Obtaining an object from the class `SparkETLManager`"
    ![](https://raw.githubusercontent.com/ThiagoPanini/sparksnake/main/docs/assets/gifs/sparksnake-setup-spark_manager.gif)

:star:{ .heart } From now on, demonstrations on some of the class's key features will be provided to users. Everything takes place from `spark_manager` object as a way for calling methods and attributes that handle the most common operations find on Glue jobs.

___

## Initializing a Glue Job

After the initial script setup, the subsequent stesp of building the application involves initializing a Glue job and obtaining all of its required elements.

Remember `glueContext`, `SparkContext` e `session`? With the `init_job()` method it's possible to get all of them as class attributes with a single line of code.

??? example "Initializing and getting all the elements required by a Glue job"
    :clapper: **Demonstration:**

    ![](https://raw.githubusercontent.com/ThiagoPanini/sparksnake/main/docs/assets/gifs/sparksnake-init_job.gif)

    ___

    :muscle: **Advantages and benefits:**
    
    - [x] Obtaining all the elements required to run a Glue job
    - [x] Automatic association of the elements as class attributes (see optional demo)
    - [x] Provides a detailed log message with useful information about the job

    ___

    :snake: **Code:**
    
    ```python
    # Initializing and getting all elements required by a Glue job
    spark_manager.init_job()
    ```

    ___

    :thinking: Learn more at [GlueJobManager.init_job()](../../mkdocstrings/gluejobmanager/#sparksnake.glue.GlueJobManager.init_job)

    ___

    ??? tip "Optional: analyzing job required elements gotten by the method"
        As mentioned, the `init_job()` method is responsible for collecting and associating the Spark context, Glue context, and Spark session elements as attributes of the instantiated class. These three elements form the basis for performing Glue features throughout a job.

        ![](https://raw.githubusercontent.com/ThiagoPanini/sparksnake/main/docs/assets/gifs/sparksnake-opt-contexto-sessao.gif)


## Reading Multiple Spark DataFrames

Following the demo journey, after the entire process of setting up and initializing and obtaining the job's elements, the time to read the source data has finally come. For that, the `data_dict` attributes will be used on the `generate_dataframes_dict()` method for providing a way to read multiple Spark DataFrames with a single line of code.

??? example "Getting a dictionary of Spark DataFrames objects"
    :clapper: **Demonstration:**

    ![](https://raw.githubusercontent.com/ThiagoPanini/sparksnake/main/docs/assets/gifs/sparksnake-generate_dataframes_dict.gif)

    ___

    :muscle: **Advantages and benefits:**
    
    - [x] Obtaining multiple Spark DataFrames with a single line of code
    - [x] Better observability through detailed log messages
    - [x] Better code organization as the number of sources grows
    - [x] Possibility of automatic creation of temp views for mapped data sources

    ___

    :snake: **Code:**
    
    ```python
    # Reading multiple Spark DataFrames at once
    dfs_dict = spark_manager.generate_dataframes_dict()
    ```

    ___

    :thinking: Learn more at [GlueJobManager.generate_dataframes_dict()](../../mkdocstrings/GlueJobManager/#sparksnake.glue.GlueJobManager.generate_dataframes_dict)

    ___

    ??? tip "Optional: Analyzing the dictionary collected and unpacking DataFrames"
        After the method `generate_dataframes_dict()` is run, the user has a dictionary that maps variable keys to objects of DataFrame type. To get the DataFrames individually in the application, you must "unpack" the resulting dictionary as follows:

        ![](https://raw.githubusercontent.com/ThiagoPanini/sparksnake/main/docs/assets/gifs/sparksnake-opt-generate_dataframes_dict.gif)


## What More Can Be Done?

Once the Spark DataFrames (or Glue DynamicFrames) has been obtained in the application, a sea of possibilities is opened. At this point, the user can apply their own transformation methods, queries in SparkSQL or any other operation that makes sense within their own business rules.

:material-alert-decagram:{ .mdx-pulse .warning } The *sparksnake*, as a library, does not claim to encapsulate all existing business rules within a Glue job. This would be virtually impossible. However, one of the advantages of *sparksnake* is to enable some common features that can be extremely useful within the journey of code development and application of business rules. And that's what you will see in the following sections.


## Extraindo Atributos de Data

Uma dessas funcionalidades citadas permite que os usuários enriqueçam um DataFrame Spark com uma série de atributos temporais a partir de uma coluna de data já existente. Em outras palavras, esta é uma forma fácil e eficiente de obter informações como o ano, mês, dia, quadrimestre, dia da semana e uma série de outras características com base em um atributo de data do DataFrame.

Para visualizar esta funcionalidade na prática, vamos criar uma versão simplificada do DataFrame `df_orders` com apenas alguns atributos essenciais para a demonstração.

??? tip "Preparação: criando versão simplificada do DataFrame df_orders"

    ![](https://raw.githubusercontent.com/ThiagoPanini/sparksnake/main/docs/assets/gifs/sparksnake-prep-df_orders_simp.gif)

Assim, o DataFrame alvo da demonstração desta funcionalidade de extração de atributos temporais contém apenas duas colunas: order_id e dt_compra. O objetivo final será obter uma série de atributos temporais com base na variável dt_compra.

??? example "Enriquecendo um DataFrame Spark com atributos temporais"
    :clapper: **Demonstração:**

    ![](https://raw.githubusercontent.com/ThiagoPanini/sparksnake/main/docs/assets/gifs/sparksnake-extract_date_attributes.gif)

    ___

    :muscle: **Vantagens e benefícios da funcionalidade:**
    
    - [x] Possibilidade de enriquecimento completo de DataFrame com informações de data
    - [x] Possibilidade de conversão automática de strings em campos date ou timestamp
    - [x] Aprimoramento de análise de dados com base em informações de data extraídas

    ___

    :snake: **Código utilizado:**
    
    ```python
    # Criando versão simplificada do DataFrame de pedidos
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

        ![](https://raw.githubusercontent.com/ThiagoPanini/sparksnake/main/docs/assets/gifs/sparksnake-opt-extract_date_attributes.gif)


## Extraindo Atributos Estatísticos

Uma outra funcionalidade extremamente interessante encapsulada no *sparksnake* está relacionada à extração de uma série de atributos estatísticos de uma coluna numérica presente em um DataFrame Spark. Com apenas uma chamada de método, o usuário poderá aplicar um complexo processo de agrupamento para enriquecer seus dados com atributos relevantes dentro de seu processo analítico.

Para a demonstração proposta, vamos utilizar um DataFrame alternativo que contempla dados de pagamentos realizados em pedidos online.

??? tip "Preparação: visualizando DataFrame de exemplo"
    ![](https://raw.githubusercontent.com/ThiagoPanini/sparksnake/main/docs/assets/gifs/sparksnake-prep-df_payments.gif)

As colunas dispostas indicam uma possibilidade analítica interessante relacionada aos valores de pagamentos realizados para cada categoria diferente. Seria interessante analisar a soma, a média, os valores mínimos e máximo, por exemplo, de pagamentos realizados em cartões de crédito e nas demais categorias. Vamos fazer isso com uma única chamada de método!

??? example "Enriquecendo DataFrame com atributos estatísticos"
    :clapper: **Demonstração:**

    ![](https://raw.githubusercontent.com/ThiagoPanini/sparksnake/main/docs/assets/gifs/sparksnake-extract_aggregate_statistics.gif)

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

    ![](https://raw.githubusercontent.com/ThiagoPanini/sparksnake/main/docs/assets/gifs/sparksnake-drop_partition.gif)

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

    :thinking: Saiba mais em [GlueJobManager.drop_partition()](../../mkdocstrings/GlueJobManager/#sparksnake.glue.GlueJobManager.drop_partition)

## Adicionando Partições à DataFrames

Em processos ETL, é comum ter operações que geram conjuntos de dados particionados por atributos já existentes e/ou por atributos de data que referem-se ao instante temporal de execução do *job*. Para casos onde precisa-se adicionar uma ou mais colunas de partição em um DataFrame Spark já existente, o método `add_partition_column()` encapsula a execução do método `withColumn()` para adicionar uma coluna na coleção distribuída de dados.

??? example "Adicionando coluna de partição em um DataFrame Spark"
    :clapper: **Demonstração:**

    ![](https://raw.githubusercontent.com/ThiagoPanini/sparksnake/main/docs/assets/gifs/sparksnake-add_partition.gif)

    ___

    :muscle: **Vantagens e benefícios da funcionalidade:**
    
    - [x] Abstração do método de adição de coluna em um DataFrame
    - [x] Maior clareza de operações em um fluxo de ETL ao explicitar a "adição de partição" como um método
    - [x] Possibilidade de combinar os métodos `drop_partition()` e `add_partition_column()` em um fluxo de carga

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

    :thinking: Saiba mais em [SparkETLManager.add_partition_column()](../../mkdocstrings/SparkETLManager/#sparksnake.manager.SparkETLManager.add_partition_column)

## Reparticionando um DataFrame

Em alguns fluxos de trabalho, pensar em maneiras de otimizar o armazenamento dos conjuntos distribuídos é essencial para garantir a eficiência e promover melhores práticas de consumo dos produtos de dados gerados no processo. Aplicar métodos de reparticionamento de DataFrames Spark contribuem para uma série de fatores positivos em fluxos de ETL e, na biblioteca *sparksnake*, o método `repartition_dataframe()` foi criado para auxiliar o usuário neste processo. Basta fornecer um número alvo de partições e a própria funcionalidade irá gerenciar qual método do Spark é mais adequado para o caso (`coalesce()` o `repartition()`).

??? example "Modificando o número de partições físicas de um DataFrame"
    :clapper: **Demonstração:**

    ![](https://raw.githubusercontent.com/ThiagoPanini/sparksnake/main/docs/assets/gifs/sparksnake-repartition.gif)

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

        ![](https://raw.githubusercontent.com/ThiagoPanini/sparksnake/main/docs/assets/gifs/sparksnake-opt-repartition.gif)

## Escrevendo e Catalogando Dados

Por fim, vamos supor agora que o usuário da biblioteca tenha a missão de escrever e catalogar os resultados das transformações codificadas. No Glue, de maneira nativa, os métodos necessários a serem chamados são:

- `.getSink()`
- `.setCatalogInfo()`
- `.writeFrame()`

Na visão do usuário, seria muito fácil se um único método pudesse ser executado para abstrair e encapsular todos os procedimentos necessários para escrita de um dado no S3 e sua posterior catalogação no Data Catalog da AWS. Para isso, o método `write_and_catalog_data()` se faz presente!

??? example "Escrevendo dados no S3 e catalogando no Data Catalog"
    :clapper: **Demonstração:**

    ![](https://raw.githubusercontent.com/ThiagoPanini/sparksnake/main/docs/assets/gifs/sparksnake-write_and_catalog_data.gif)

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

    # Escrevendo dados no S3 e catalogando no Data Catalog
    spark_manager.write_and_catalog_data(
        df=df_payments_partitioned,
        s3_table_uri=s3_table_uri,
        output_database_name="ra8",
        output_table_name="tbl_payments",
        partition_name="anomesdia",
        output_data_format="csv"
    )
    ```
    ___

    :thinking: Saiba mais em [GlueJobManager.write_and_catalog_data()](../../mkdocstrings/GlueJobManager/#sparksnake.glue.GlueJobManager.write_and_catalog_data)

