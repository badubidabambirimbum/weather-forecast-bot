import logging

from aiogram import Bot, types, Dispatcher, executor
from typing import Literal
# import secret.auth_data as s  # API KEY, ADMIN ID, LOG ID, ...
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardRemove
from datetime import datetime
import asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import sys
import os
from dotenv import load_dotenv
from io import BytesIO

# sys.path.append('/app/src/telegram_bot/utils')
# sys.path.append('/app/src/core')

import telegram_bot.utils.additional_functions as lib
from telegram_bot.utils.telegram_constants import WEATHER_YANDEX_SMILE, WEATHER_GISMETEO_SMILE, WEATHER_GISMETEO_EXCEPTIONS, SET_CITIES, SET_TYPES, TRANSLATE_CITIES
from telegram_bot.utils.Keyboards import kb, kb_help, kb_cities, ikb_info

from core.database import DataBase
from core.logger import create_logger, log_function

load_dotenv('../../secret/config.env')
load_dotenv('../../secret/secret.env')

bot = Bot(os.getenv("TOKEN"))
dp = Dispatcher(bot)
logger, LOG_FILENAME = create_logger()

log_id = os.getenv("TELEGRAM_CHAT_ID")
admin_id = int(os.getenv("ADMIN_ID"))

scheduler_async = AsyncIOScheduler()

db = DataBase(
            host=os.getenv('POSTGRES_HOST'),
            port=os.getenv('POSTGRES_PORT'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD'),
            database=os.getenv('POSTGRES_DB'),
            logger=logger
            )

logger.info(f"PWD: {os.getcwd()}")
logger.info(f"sys.path: {sys.path}")
logger.info(f"LOG_FILENAME: {LOG_FILENAME}")


@log_function(logger=logger)
async def scheduled_notification():
    '''
    Отправка прогноза подписчикам
    '''
    select_sub = f"SELECT * FROM prom.subscribers;"
    rows = db.execute_query(select_sub)
    for row in rows:
        try:
            await bot.send_message(row["id"], text=lib.create_forecast(row["city"], 1, 1, db, 'model'), parse_mode='HTML')
        except Exception as e:
            logger.error(f"Не удалось отправить сообщение пользователю {row['id']}: {e}")
            await bot.send_message(chat_id=log_id,
                                   text=f"❌ Не удалось отправить сообщение пользователю {row['id']}: {e}",
                                   parse_mode='HTML')


def start_scheduler_async():
    '''
    Создание расписания для автоматического запуска функций
    '''
    scheduler_async.add_job(scheduled_notification, 'cron', hour=os.getenv("TG_HOUR"), minute=os.getenv("TG_MINUTE"))
    scheduler_async.start()


@log_function(logger=logger)
async def add_user(city: Literal['Moscow', 'Ekaterinburg', 'Krasnodar', None], message: types.Message):
    '''
    Добавление подписчика
    '''
    user_id = message.from_user.id

    # Поиск индекса в таблице
    select_sub = "SELECT id FROM prom.subscribers WHERE id = {user_id};".format(user_id=str(user_id))
    rows = db.execute_query(select_sub)

    if len(rows) == 0:
        db.insert('prom',
                  'subscribers',
                  ['id', 'date', 'city'],
                  (str(user_id), str(datetime.now().strftime('%Y-%m-%d')), str(city))
                  )

        logger.info(f"Новая подписка на оповещение о погоде: {message.from_user.username}")
        await bot.send_message(chat_id=log_id,
                               text=f"✅ Новая подписка на оповещение о погоде: {message.from_user.username}",
                               parse_mode='HTML')

        await message.reply("✅ Отлично!😄\n"
                            "Каждый день в 7️⃣ утра по МСК бот🤖 будет присылать вам прогноз погоды на предстоящий день!😉")
    else:
        await message.reply("Вы уже подписаны на оповещение о погоде❗️️")


@log_function(logger=logger)
async def on_startup(_):
    '''
    Функция, которая вызывается при запуске бота
    '''
    await bot.send_message(log_id, text=f"🤖 <b>запущен</b>!", parse_mode='HTML')
    if db.connection:
        await bot.send_message(chat_id=log_id, text="✅ Подключение установлено!", parse_mode='HTML')
    else:
        await bot.send_message(chat_id=log_id, text=f"❌ Не удалось установить подключение!\n{db.log_error_connect}", parse_mode='HTML')
        sys.exit(0)
    logger.info('Бот запущен {current_time}'.format(current_time=datetime.now()))


@log_function(logger=logger)
async def on_shutdown(_):
    '''
    Функция, которая вызывается при выключении бота
    '''
    await bot.send_message(log_id, text=f"🤖 <b>выключен</b>!", parse_mode='HTML')
    try:
        db.close_connection()
    except:
        pass
    logger.info('Бот выключен {current_time}'.format(current_time=datetime.now()))


@dp.message_handler(commands=["help"])
@log_function(logger=logger)
async def help_message(message: types.Message):
    logger.info(f'called by the user {message.from_user.id}')
    await bot.send_sticker(chat_id=message.from_user.id,
                           sticker="CAACAgIAAxkBAAEMj1Fmp6-tcw1DpXSWJp3yCkcgTFAy6QACshIAAmD9iUtRNBJT06z1kDUE",
                           reply_markup=ReplyKeyboardRemove())

    text_help = '❓ <b>Как получить прогноз?</b> \n' \
                '<b>1.</b> <i>Введи название города</i> 🏙 \n' \
                '<b>2.</b> <i>Выбери количество дней</i> 📅 \n' \
                '<b>3.</b> <i>Готово! Наслаждайся прогнозом!</i> 😉 \n' \
                ' \n' \
                '❓ <b>Как узнать доступные города?</b> \n' \
                '▪️<i>Введи</i> <b>/cities</b> \n' \
                ' \n' \
                '❓ <b>Что означают смайлики погоды?</b> \n' \
                '▪️<i>Введи</i> <b>/weather</b> \n' \
                ' \n' \
                '❓ <b>Можно ли выбрать другой источник прогноза температуры?</b> \n' \
                '▪️<i>Да, надо ввести команду</i> <b>/update (название источника)</b> \n' \
                '▪️<i>В данный момент доступно 3 источника:</i> <b>Model</b>, <b>Yandex</b>, <b>GisMeteo</b> \n' \
                ' \n' \
                '❓ <b>А у вас есть возможность рассылки✉️ прогноза?</b> \n' \
                '▪️<i>Конечно! для подписки на рассылку введи</i> <b>+(Название города)</b> \n' \
                '▪️<i>Пример: +Москва</i> \n' \
                '▪️<i>Для отмены рассылки введи</i> <b>/remove</b> \n' \
                ' \n' \
                '❗️ <b>Информация о проекте:</b> \n' \
                '▪️ <i>Введи</i> <b>/info</b>'

    await bot.send_message(
        chat_id=message.from_user.id,
        text=text_help,
        parse_mode='HTML',
        reply_markup=kb_cities)


@dp.message_handler(commands=["info"])
@log_function(logger=logger)
async def info_message(message: types.Message):
    logger.info(f'called by the user {message.from_user.id}')
    await bot.send_sticker(message.from_user.id,
                           sticker="CAACAgEAAxkBAAEMrDFmxDk66eDeDYk0jqiSZvGBeX2klAAC0gMAAuJ5IET7lNR5d0OiyjUE")
    await bot.send_message(chat_id=message.from_user.id,
                           text='Ниже представлены ссылка на GitHub проекта📄 и на мой Telegram для обратной связи📞',
                           reply_markup=ikb_info)


@dp.message_handler(commands=["start"])
@log_function(logger=logger)
async def start_message(message: types.Message):
    logger.info(f'called by the user {message.from_user.id}')
    user_id = message.from_user.id

    # Поиск индекса в таблице
    select_sub = "SELECT id FROM prom.all_users WHERE id = {user_id};".format(user_id=str(user_id))
    rows = db.execute_query(select_sub)

    if len(rows) == 0:
        db.insert('prom',
                  'all_users',
                  ['id', 'username', 'id_source', 'date'],
                  (str(user_id), str(message.from_user.username), '1', str(datetime.now().strftime('%Y-%m-%d')))
                  )

        logger.info(f"Новый пользователь: {message.from_user.username}!!!")
        await bot.send_message(chat_id=log_id,
                               text=f"🆕 Новый пользователь: {message.from_user.username}!!!",
                               parse_mode='HTML')

    await bot.send_sticker(message.from_user.id,
                           sticker="CAACAgIAAxkBAAEMj01mp68a2RxE2V-27EZhT1TxljV3zQACjRAAAl_bkUp3Bt1MNp18SzUE",
                           reply_markup=kb_help)
    await message.answer(
        f'Привет, {message.from_user.first_name}!👋 \n'
        f'Здесь ты найдешь ближайший прогноз погоды 🌦 в интересующем тебя городе! \n'
        f'Для дополнительной информации воспользуйся командой /help')


@dp.message_handler(commands=["cities"])
@log_function(logger=logger)
async def cities_message(message: types.Message):
    logger.info(f'called by the user {message.from_user.id}')
    await message.answer(text=f"Доступные города ⚡️", reply_markup=kb)


@dp.message_handler(commands=["update"])
@log_function(logger=logger)
async def cities_message(message: types.Message):
    logger.info(f'called by the user {message.from_user.id}')

    args = message.get_args()

    try:
        source_name_new = str(args).lower()

        if not source_name_new:
            logger.warning("Получена пустая строка")
            raise ValueError("Получена пустая строка")

        row_new = db.execute_query(query=f'''select id_source from prom.forecast_sources where source_name = '{source_name_new}' ''')
        id_source_new = row_new[0]['id_source']

        row_old = db.execute_query(
            query=f'''
            select 
                a.id_source, f.source_name
            from 
                prom.all_users as a
            left join 
                prom.forecast_sources as f
            on
                a.id_source = f.id_source
            where 
                id = {str(message.from_user.id)}''')

        id_source_old = row_old[0]['id_source']
        source_name_old = row_old[0]['source_name']

        if id_source_old == id_source_new:
            await message.reply(f"Вы уже используете источник {source_name_old}!")
        else:
            db.update(schema='prom', table_name='all_users', set_query=f'id_source = {str(id_source_new)}', filter=f"id = {str(message.from_user.id)}")
            await message.reply(f"Вы успешно изменили источник с {source_name_old} на {source_name_new}!")

    except ValueError:
        await message.reply("Что-то пошло не так. Пример использования: /update model")


@dp.message_handler(commands=["weather"])
@log_function(logger=logger)
async def weather_message(message: types.Message):
    logger.info(f'called by the user {message.from_user.id}')
    mes_ya = "Обозначения погоды 🔸Yandex:\n\n"
    mes_gis = "Обозначения погоды 🔹GisMeteo:\n\n"

    for weather in WEATHER_YANDEX_SMILE:
        mes_ya += weather + " " + WEATHER_YANDEX_SMILE[weather] + "\n"
    for weather in WEATHER_GISMETEO_SMILE:
        if weather not in WEATHER_GISMETEO_EXCEPTIONS:
            mes_gis += weather + " " + WEATHER_GISMETEO_SMILE[weather] + "\n"

    await bot.send_sticker(message.from_user.id,
                           sticker="CAACAgEAAxkBAAEMj_RmqKuKC9rmnTElJX3QEr-MYpC-cAACXQMAApzteUVTI9qtaJq7kTUE",
                           reply_markup=kb_cities)
    await message.answer(text=mes_ya)
    await message.answer(text=mes_gis)


@dp.message_handler(lambda message: '+москва' == message.text.lower())
@log_function(logger=logger)
async def add_Moscow(message: types.Message):
    logger.info(f'called by the user {message.from_user.id}')
    await add_user(message.text[1:].capitalize(), message)


@dp.message_handler(lambda message: '+краснодар' == message.text.lower())
@log_function(logger=logger)
async def add_Krasnodar(message: types.Message):
    logger.info(f'called by the user {message.from_user.id}')
    await add_user(message.text[1:].capitalize(), message)


@dp.message_handler(lambda message: '+екатеринбург' == message.text.lower())
@log_function(logger=logger)
async def add_Ekaterinburg(message: types.Message):
    logger.info(f'called by the user {message.from_user.id}')
    await add_user(message.text[1:].capitalize(), message)


@dp.message_handler(commands=["remove"])
@log_function(logger=logger)
async def remove_message(message: types.Message):
    logger.info(f'called by the user {message.from_user.id}')
    user_id = message.from_user.id

    select_sub = "SELECT id FROM prom.subscribers WHERE id = {user_id};".format(user_id=str(user_id))
    rows = db.execute_query(select_sub)

    if len(rows) != 0:

        filter = 'id = {user_id}'.format(user_id=str(user_id))
        db.delete('prom', 'subscribers', filter)

        logger.warning(f"Отписка: {message.from_user.username}")
        await bot.send_message(chat_id=log_id,
                               text=f"❗️ Отписка: {message.from_user.username}",
                               parse_mode='HTML')

        await message.reply("Вы успешно отписались от оповещения о погоде! ✔️")
    else:
        await message.reply("Вы не были подписаны на оповещение о погоде❗️")


# ADMIN
@dp.message_handler(commands=["admin"])
@log_function(logger=logger)
async def admin_list(message: types.Message):
    logger.info(f'called by the user {message.from_user.id}')
    user_id = message.from_user.id
    if user_id == admin_id:
        text = (f"<b>/check</b> - <i>проверка данных из источников прогноза погоды</i> \n"
                f"<b>/all_users</b> - <i>список всех пользователей</i> \n"
                f"<b>/subs</b> - <i>список подписчиков на рассылку</i> \n"
                f"<b>/off</b> - <i>выключить бота</i> \n"
                f"<b>/message_subs</b> - <i>рассылка прогноза погоды</i> \n"
                f"<b>/logs n</b> - <i>посмотреть последние n строк лога</i> \n")
        await bot.send_message(chat_id=admin_id,
                               text=text,
                               parse_mode='HTML')
    else:
        await bot.send_message(chat_id=user_id,
                               text=f'Данная команда вам недоступна!')


# ADMIN
@dp.message_handler(commands=["check"])
@log_function(logger=logger)
async def check_datasets(message: types.Message):
    logger.info(f'called by the user {message.from_user.id}')
    user_id = message.from_user.id
    if user_id == admin_id:
        text = ""
        for type in SET_TYPES:
            for city in SET_CITIES:
                table_name = f"t_{TRANSLATE_CITIES[city]}_{type}"
                text += f"{len(lib.view(table_name, db, key='all'))} {city} {type} \n"
        await bot.send_message(chat_id=admin_id,
                               text=text)
    else:
        await bot.send_message(chat_id=user_id,
                               text=f'Данная команда вам недоступна!')


# ADMIN
@dp.message_handler(commands=["all_users"])
@log_function(logger=logger)
async def database_all_users(message: types.Message):
    logger.info(f'called by the user {message.from_user.id}')
    user_id = message.from_user.id

    if user_id == admin_id:
        select_sub = f"SELECT * FROM prom.all_users;"
        rows = db.execute_query(select_sub)
        names = ['id', 'username', 'id_source', 'date']
        text = f"{str(names[0]):^25} {str(names[1]):^25} {str(names[2]):^15} {str(names[3]):^15}\n"

        for row in rows:
            text += f"{str(row['id']):^10} {str(row['username']):^15} {str(row['id_source']):^15} {str(row['date']):<15} \n"

        await bot.send_message(chat_id=admin_id,
                               text=text)
    else:
        await bot.send_message(chat_id=user_id,
                               text=f'Данная команда вам недоступна!')


# ADMIN
@dp.message_handler(commands=["subs"])
@log_function(logger=logger)
async def database_subs(message: types.Message):
    logger.info(f'called by the user {message.from_user.id}')
    user_id = message.from_user.id

    if user_id == admin_id:
        select_sub = f"SELECT * FROM prom.subscribers;"
        rows = db.execute_query(select_sub)
        names = ['id', 'date', 'city']
        text = f"{str(names[0]):^25} {str(names[1]):^25} {str(names[2]):^15} \n"

        for row in rows:
            text += f"{str(row['id']):^10} {str(row['date']):^15} {str(row['city']):<15} \n"

        await bot.send_message(chat_id=admin_id,
                               text=f'<code>{text}</code>',
                               parse_mode='HTML')
    else:
        await bot.send_message(chat_id=user_id,
                               text=f'Данная команда вам недоступна!')


# ADMIN
@dp.message_handler(commands=["off"])
@log_function(logger=logger)
async def off_bot(message: types.Message):
    logger.info(f'called by the user {message.from_user.id}')
    user_id = message.from_user.id
    if user_id == admin_id:
        await bot.send_message(log_id, text=f"🤖 <b>выключен</b>!", parse_mode='HTML')
        db.close_connection()
        logging.warning(f"{datetime.now()} Бот выключен!")
        sys.exit(0)
    else:
        await bot.send_message(chat_id=user_id,
                               text=f'Данная команда вам недоступна!')


# ADMIN
@dp.message_handler(commands=["message_subs"])
@log_function(logger=logger)
async def message_for_subs(message: types.Message):
    logger.info(f'called by the user {message.from_user.id}')
    user_id = message.from_user.id
    if user_id == admin_id:
        await scheduled_notification()
    else:
        await bot.send_message(chat_id=user_id,
                               text=f'Данная команда вам недоступна!')


# ADMIN
@dp.message_handler(commands=["logs"])
@log_function(logger=logger)
async def get_logs(message: types.Message):
    logger.info(f'called by the user {message.from_user.id}')
    user_id = message.from_user.id
    if user_id == admin_id:
        args = message.get_args()

        try:
            n = int(args) if args else 100
        except ValueError:
            await message.reply("Использование: /logs 200")
            return

        n = min(n, 5000)

        lines = lib.get_tail_file(f"logs/{LOG_FILENAME}", n)

        log_text = "".join(lines)

        buffer = BytesIO(log_text.encode("utf-8")) # Создаём объект BytesIO, который ведёт себя как файл, но хранится в памяти.
        buffer.name = f"logs_last_{n}.txt" # Даем ему название
        buffer.seek(0) # Перемещает “указатель” в начало файла. Если так не сделать, то отправим пустой файл

        await message.reply_document(buffer)
    else:
        await bot.send_message(chat_id=user_id,
                               text=f'Данная команда вам недоступна!')


@dp.message_handler()
@log_function(logger=logger)
async def check_message(message: types.Message):
    logger.info(f'called by the user {message.from_user.id}')
    ikb = InlineKeyboardMarkup(row_width=3)
    ib1 = InlineKeyboardButton(text="1️⃣", callback_data=message.text + " 1")
    ib2 = InlineKeyboardButton(text="3️⃣", callback_data=message.text + " 3")
    ib3 = InlineKeyboardButton(text="🔟", callback_data=message.text + " 10")
    ikb.add(ib1, ib2, ib3)

    if message.text in SET_CITIES:
        await bot.send_message(chat_id=message.from_user.id,
                               text='Выберите дальность прогноза!',
                               reply_markup=ikb)
    else:
        await bot.send_sticker(message.from_user.id,
                               sticker="CAACAgIAAxkBAAEMj1Zmp7BJuY6OS5U-NOvcJoe-vZYHAQACSBEAAsHxIEtc_h1kap2wijUE",
                               reply_markup=kb_help)
        await message.reply('Я вас не понимаю 😔 \n'
                            'Воспользуйтесь, пожалуйста, командой /help')
    logger.info('_stopped check_message. user:{user_id} {current_time}'.format(current_time=datetime.now(),
                                                                                user_id=message.from_user.id))


@dp.callback_query_handler()
@log_function(logger=logger)
async def callback_message(callback: types.CallbackQuery):
    await callback.message.delete_reply_markup()
    city, dist = callback.data.split()
    try:
        user_id = str(callback.from_user.id)
        row = db.execute_query(
            query=f'''
            select 
                f.source_name
            from 
                prom.all_users as a
            left join 
                prom.forecast_sources as f
            on
                a.id_source = f.id_source
            where 
                id = {user_id}''')

        source_name = row[0]['source_name']

        forecast_txt = lib.create_forecast(city, dist, 10, db, source_name)
    except Exception as e:
        logger.error(e)
        forecast_txt = f"Прогноз погоды на {dist} дней недоступен!"

    await bot.send_message(callback.from_user.id, text=f'Прогноз в городе {city} на {dist} дней:')
    await bot.send_message(callback.from_user.id, text=forecast_txt, parse_mode='HTML')



if __name__ == "__main__":
    start_scheduler_async()
    executor.start_polling(dp, on_startup=on_startup, skip_updates=True, on_shutdown=on_shutdown)
