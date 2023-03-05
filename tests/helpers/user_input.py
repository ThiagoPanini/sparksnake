"""Consolidando inputs do usuário para criação de objetos de classes.

Este arquivo Python tem como principal objetivo centralizar a definição
de variáveis utilizadas como simulação de entradas de usuários para a
criação e configuração de objetos de classes presentes nos módulos
que consolidam as funcionalidades da biblioteca.

A ideia por trás de centralizar tais definições em um único arquivo é
a de proporcionar um maior nível de organização no que diz respeito
a criação de fixtures e insumos para a execução de testes unitários.
Com isso, qualquer tipo de alteração realizada nas definições aqui
consolidadas poderão refletir instantaneamente no comportamento dos
elementos criados a partir das mesmas.

___
"""

# Simulando lista de argumentos de um job Glue
FAKE_ARGV_LIST = ["JOB_NAME", "S3_SOURCE_PATH", "S3_OUTPUT_PATH"]

# Simulando dicionário de origens utilizado nas classes da biblioteca
FAKE_DATA_DICT = {
    "orders": {
        "database": "some-fake-database",
        "table_name": "orders-fake-table",
        "transformation_ctx": "dyf_orders"
    },
    "customers": {
        "database": "some-fake-database",
        "table_name": "customers-fake-table",
        "transformation_ctx": "dyf_customers",
        "push_down_predicate": "anomesdia=20221201",
        "create_temp_view": True,
        "additional_options": {
            "compressionType": "lzo"
        }
    }
}