import logging
import os
import sys
from datetime import datetime

class Logger:
    def __init__(self, log_dir='logs', log_level=logging.DEBUG):
        """
        Инициализация логгера.

        :param log_dir: директория для хранения логов
        :param log_level: уровень логирования
        """
        self.log_dir = log_dir
        os.makedirs(self.log_dir, exist_ok=True)

        # Имя вызывающего скрипта без расширения
        script_name = os.path.splitext(os.path.basename(sys.argv[0]))[0]

        # Формируем имя лог файла
        log_filename = f"{script_name}-{datetime.now().strftime('%Y-%m-%d')}.log"
        log_file_path = os.path.join(self.log_dir, log_filename)

        # Создаем логгер
        self.logger = logging.getLogger(log_file_path)
        self.logger.setLevel(log_level)
        self.logger.propagate = False

        if not self.logger.handlers:
            # Формат сообщений
            formatter = logging.Formatter(
                fmt='[%(asctime)s] [%(levelname)s] %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )

            # Хендлер для записи в файл
            file_handler = logging.FileHandler(log_file_path, encoding='utf-8')
            file_handler.setFormatter(formatter)

            self.logger.addHandler(file_handler)

    def get_logger(self):
        return self.logger