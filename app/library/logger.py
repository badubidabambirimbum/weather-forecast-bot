import logging
import os
import sys
from datetime import datetime

def create_logger(log_dir='logs', log_level=logging.DEBUG):
    """
    Инициализация логгера.

    :param log_dir: директория для хранения логов
    :param log_level: уровень логирования
    """
    log_dir = log_dir
    os.makedirs(log_dir, exist_ok=True) # создаём папку

    # Имя вызывающего скрипта без расширения
    script_name = os.path.splitext(os.path.basename(sys.argv[0]))[0]

    # Формируем имя лог файла
    log_filename = f"{script_name}_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"
    log_file_path = os.path.join(log_dir, log_filename)

    # Создаем логгер
    logger = logging.getLogger(log_file_path)
    logger.setLevel(log_level)
    logger.propagate = False

    if not logger.handlers:
        # Формат сообщений
        formatter = logging.Formatter(
            fmt='[%(asctime)s][%(levelname)s][%(funcName)s][%(message)s]',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        # Хендлер для записи в файл
        file_handler = logging.RotatingFileHandler(log_file_path,
                                                   maxBytes=100_000_000,  # 100
                                                   MBbackupCount=100,
                                                   encoding="utf-8"
                                                   )
        # file_handler = logging.FileHandler(log_file_path, encoding='utf-8')
        file_handler.setFormatter(formatter)

        logger.addHandler(file_handler)

    return logger, log_filename