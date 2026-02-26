import logging
import logging.handlers
import os
import sys
from datetime import datetime
import functools

def create_logger(log_dir='logs', log_level=logging.DEBUG) -> tuple[logging.Logger, str]:
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
    log_filename = f"{script_name}_{datetime.now().strftime('%Y-%m-%d')}.log"
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
        file_handler = logging.handlers.RotatingFileHandler(
                                                   filename=log_file_path,
                                                   maxBytes=100_000_000,  # 100 MB
                                                   backupCount=100,
                                                   encoding="utf-8"
                                                   )
        # file_handler = logging.FileHandler(log_file_path, encoding='utf-8')
        file_handler.setFormatter(formatter)

        out_handler = logging.StreamHandler(sys.stdout)
        out_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(out_handler)

    return logger, log_filename

def log_function(logger):
    """Декоратор для логирования выполнения функций"""
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            logger.info(f"_started '{func.__name__}'")
            try:
                result = await func(*args, **kwargs)
                logger.info(f"_stopped '{func.__name__}'")
                return result
            except Exception as e:
                logger.error(f"_failed '{func.__name__}' {e}")
        return wrapper
    return decorator

def log_class_method(func):
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        self.logger.info(f"_started '{func.__name__}'")
        try:
            result = func(*args, **kwargs)
            self.logger.info(f"_stopped '{func.__name__}'")
            return result
        except Exception as e:
            self.logger.error(f"_failed '{func.__name__}' {e}")
    return wrapper