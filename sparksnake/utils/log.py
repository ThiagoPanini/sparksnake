"""
Módulo responsável por auxiliar na configuração de objetos
de log instanciados via Python e utilizados no decorrer
dos módulos e funções para proporcionar um melhor
observability dos processos.

___
"""

# Importando bibliotecas
import logging


# Função para configuração de log
def log_config(
    logger_name: str = __file__,
    logger_level: int = logging.INFO,
    logger_date_format: str = "%Y-%m-%d %H:%M:%S"
) -> logging.Logger:
    """
    Configuração de logs.

    Função criada para facilitar a criação de configuração
    de uma instância de Logger do Python utilizada no
    decorrer da aplicação Spark para registros de logs
    das atividades e das funcionalidades desenvolvidas.

    Examples:
        # Importando módulo
        from utils.log import log_config

        # Instanciando e configurando logger
        logger = log_config(logger)

    Args:
        logger_name (str): Referência do objeto logger criado
        logger_level (int): Tipo padrão do logger configurado
        logger_date_format (str): Formato padrão de datas das mensagens

    Returns:
        Objeto logger já configurado para o usuário
    """

    # Instanciando objeto de logging
    logger = logging.getLogger(logger_name)
    logger.setLevel(logger_level)

    # Configurando formato das mensagens no objeto
    log_format = "%(levelname)s;%(asctime)s;%(filename)s;"
    log_format += "%(lineno)d;%(message)s"
    formatter = logging.Formatter(log_format,
                                  datefmt=logger_date_format)

    # Configurando stream handler do objeto de log
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    return logger
