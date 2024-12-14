import yaml

import logging
from logging.handlers import TimedRotatingFileHandler
from pythonjsonlogger import jsonlogger

import uuid
import time
import os

def read_yaml_config(file_path:str):
    try:
        with open(file_path, 'r') as file:
            config = yaml.safe_load(file)
        return config
    except FileNotFoundError:
        print(f"Ошибка: файл {file_path} не найден.")
    except yaml.YAMLError as e:
        print(f"Ошибка при разборе YAML: {e}")
    return None

# Логирование
def setup_logger(yaml_config):
    logger = logging.getLogger(os.getenv("HOSTNAME"))
    logger.setLevel(yaml_config['Logging']['Level'])

    # Хэндлер для записи в файл с ротацией
    file_handler = TimedRotatingFileHandler(yaml_config['Logging']['File'], when="midnight", backupCount=7)
    file_handler.setLevel(yaml_config['Logging']['Level'])
    formatter = jsonlogger.JsonFormatter('%(asctime)s %(levelname)s %(message)s')
    file_handler.setFormatter(formatter)

    # Хэндлер для отправки логов в Logstash
    logstash_handler = logging.handlers.SocketHandler(yaml_config['Logging']['LogstashHost'], yaml_config['Logging']['LogstashPort'])
    logstash_handler.setLevel(yaml_config['Logging']['Level'])
    logstash_formatter = jsonlogger.JsonFormatter('%(asctime)s %(levelname)s %(message)s')
    logstash_handler.setFormatter(logstash_formatter)

    logger.addHandler(file_handler)
    logger.addHandler(logstash_handler)

    return logger

# def generate_uid(user_info):
#     unique_string = f"{user_info['sub']}_{user_info['email']}_{time.time()}"
#     uid = base64.urlsafe_b64encode(unique_string.encode()).decode().strip("=")
#     return uid

def generate_uid(user_info):
    uid = user_info['username'] + str(int(time.time()))
    return uid

