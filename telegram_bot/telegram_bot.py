import time

from aiogram import Bot, types, Dispatcher, executor

import secret.auth_data as s  # API KEY, ADMIN ID, LOG ID, ...
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardRemove
from Keyboards import kb, kb_help, kb_cities, ikb_info
from datetime import datetime
import asyncio
from psycopg2.extras import RealDictCursor
from apscheduler.schedulers.asyncio import AsyncIOScheduler

import sys
import os

# sys.path.append(os.path.abspath(os.path.join(os.getcwd(), '..', 'library')))
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), '..')))

import library.additional_functions as lib
import library.airflow_functions as afl
from library.telegram_constants import WEATHER_YANDEX_SMILE, WEATHER_GISMETEO_SMILE, WEATHER_GISMETEO_EXCEPTIONS, SET_CITIES, SET_TYPES, TRANSLATE_CITIES


time.sleep(100)

bot = Bot(s.token)
dp = Dispatcher(bot)

scheduler_async = AsyncIOScheduler()

connection, connect_text = lib.create_connect(host=s.docker_host,
                                              port=s.port,
                                              user=s.user_tg,
                                              password=s.password_tg,
                                              database=s.database_tg)


# Загрузка обновлений в таблицу
async def update_dataset(city=None, type=None):
    if city == None and type == None:
        log_time = f"{datetime.now().date()}\n"
        log_string = f""
        log_count = 0
        for city_ru in SET_CITIES:
            for type in SET_TYPES:
                city = TRANSLATE_CITIES[city_ru]
                try:
                    afl.update(city, type, connection)
                    log_string += f"✅ {city} {type} \n"
                    log_count += 1
                except Exception as e:
                    print(f"Ошибка: {e}")
                    log_string += f"❌ {city} {type} \n"
                await asyncio.sleep(120)
        await bot.send_message(s.log_id, text=f"{log_time} {log_count} / 6 \n{log_string}", parse_mode='HTML')
    else:
        try:
            afl.update(city, type, connection)
            await bot.send_message(s.log_id, text=f"✅ {city} {type}", parse_mode='HTML')
        except Exception as e:
            await bot.send_message(s.log_id, text=f"❌ {city} {type}\n{e}", parse_mode='HTML')


# Отправка прогноза подписчикам
async def scheduled_notification():
    with connection.cursor(cursor_factory=RealDictCursor) as cursor:
        select_sub = f"SELECT * FROM prom.subscribers;"
        cursor.execute(select_sub)
        rows = cursor.fetchall()
    for row in rows:
        try:
            await bot.send_message(row["id"], text=lib.create_forecast(row["city"], 1, 1, connection), parse_mode='HTML')
        except Exception as e:
            await bot.send_message(chat_id=s.log_id,
                                   text=f"❌ Не удалось отправить сообщение пользователю {row['id']}: {e}",
                                   parse_mode='HTML')


# Создание расписания для автоматического запуска функций
def start_scheduler_async():
    scheduler_async.add_job(scheduled_notification, 'cron', hour=7, minute=0)
    scheduler_async.start()


# Добавление подписчика
async def add_user(city: str, message: types.Message):

    user_id = message.from_user.id

    # Поиск индекса в таблице
    with connection.cursor(cursor_factory=RealDictCursor) as cursor:
        select_sub = "SELECT id FROM prom.subscribers WHERE id = %s;"
        cursor.execute(select_sub, (user_id,))
        rows = cursor.fetchall()

    if len(rows) == 0:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            add_new_sub = "INSERT INTO prom.subscribers (id, date, city) VALUES (%s, %s, %s);"
            cursor.execute(add_new_sub, (user_id, datetime.now().strftime('%Y-%m-%d'), city))
            connection.commit()

        print(f"Новая подписка на оповещение о погоде: {message.from_user.username}")
        await bot.send_message(chat_id=s.log_id,
                               text=f"✅ Новая подписка на оповещение о погоде: {message.from_user.username}",
                               parse_mode='HTML')

        await message.reply("✅ Отлично!😄\n"
                            "Каждый день в 7️⃣ утра по МСК бот🤖 будет присылать вам прогноз погоды на предстоящий день!😉")
    else:
        await message.reply("Вы уже подписаны на оповещение о погоде❗️️")


# Функция, которая вызывается при запуске бота
async def on_startup(_):
    await bot.send_message(s.log_id, text=f"🤖 <b>запущен</b>!", parse_mode='HTML')
    await bot.send_message(chat_id=s.log_id, text=connect_text, parse_mode='HTML')
    if connect_text != f"✅ Подключение установлено!":
        sys.exit(0)
    print(f"{datetime.now()} Бот был успешно запущен!")


# Функция, которая вызывается при выключении бота
async def on_shutdown(_):
    await bot.send_message(s.log_id, text=f"🤖 <b>выключен</b>!", parse_mode='HTML')
    try:
        connection.close()
    except:
        pass
    print(f"{datetime.now()} Бот выключен!")


@dp.message_handler(commands=["start"])
async def start_message(message: types.Message):
    user_id = message.from_user.id

    # Поиск индекса в таблице
    with connection.cursor(cursor_factory=RealDictCursor) as cursor:
        select_sub = f"SELECT id FROM prom.all_users WHERE id = %s;"
        cursor.execute(select_sub, (user_id,))
        rows = cursor.fetchall()

    if len(rows) == 0:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            add_new_user = "INSERT INTO prom.all_users (id, username, date) VALUES (%s, %s, %s);"
            cursor.execute(add_new_user, (user_id, message.from_user.username, datetime.now().strftime('%Y-%m-%d')))
            connection.commit()

        print(f"Новый пользователь: {message.from_user.username}!!!")
        await bot.send_message(chat_id=s.log_id,
                               text=f"🆕 Новый пользователь: {message.from_user.username}!!!",
                               parse_mode='HTML')

    await bot.send_sticker(message.from_user.id,
                           sticker="CAACAgIAAxkBAAEMj01mp68a2RxE2V-27EZhT1TxljV3zQACjRAAAl_bkUp3Bt1MNp18SzUE",
                           reply_markup=kb_help)
    await message.answer(
        f'Привет, {message.from_user.first_name}!👋 \n'
        f'Здесь ты найдешь ближайший прогноз погоды 🌦 в интересующем тебя городе! \n'
        f'Для дополнительной информации воспользуйся командой /help')


@dp.message_handler(commands=["help"])
async def help_message(message: types.Message):
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
async def info_message(message: types.Message):
    await bot.send_sticker(message.from_user.id,
                           sticker="CAACAgEAAxkBAAEMrDFmxDk66eDeDYk0jqiSZvGBeX2klAAC0gMAAuJ5IET7lNR5d0OiyjUE")
    await bot.send_message(chat_id=message.from_user.id,
                           text='Ниже представлены ссылка на GitHub проекта📄 и на мой Telegram для обратной связи📞',
                           reply_markup=ikb_info)


@dp.message_handler(commands=["cities"])
async def cities_message(message: types.Message):
    await message.answer(text=f"Доступные города ⚡️", reply_markup=kb)


@dp.message_handler(commands=["weather"])
async def weather_message(message: types.Message):
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
async def add_Moscow(message: types.Message):
    await add_user(message.text[1:].capitalize(), message)


@dp.message_handler(lambda message: '+краснодар' == message.text.lower())
async def add_Krasnodar(message: types.Message):
    await add_user(message.text[1:].capitalize(), message)


@dp.message_handler(lambda message: '+екатеринбург' == message.text.lower())
async def add_Ekaterinburg(message: types.Message):
    await add_user(message.text[1:].capitalize(), message)


@dp.message_handler(commands=["remove"])
async def remove_message(message: types.Message):
    user_id = message.from_user.id

    with connection.cursor(cursor_factory=RealDictCursor) as cursor:
        select_sub = f"SELECT id FROM prom.subscribers WHERE id = %s;"
        cursor.execute(select_sub, (user_id,))
        rows = cursor.fetchall()

    if len(rows) != 0:

        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            select_sub = f"DELETE FROM prom.subscribers WHERE id = %s;"
            cursor.execute(select_sub, (user_id,))
            connection.commit()

        print(f"Отписка: {message.from_user.username}")
        await bot.send_message(chat_id=s.log_id,
                               text=f"❗️ Отписка: {message.from_user.username}",
                               parse_mode='HTML')

        await message.reply("Вы успешно отписались от оповещения о погоде! ✔️")
    else:
        await message.reply("Вы не были подписаны на оповещение о погоде❗️")


# ADMIN
@dp.message_handler(commands=["admin"])
async def admin_list(message: types.Message):
    user_id = message.from_user.id
    if user_id == s.admin_id:
        text = (f"<b>/update</b> - <i>обновление базы данных</i> \n"
                f"<b>/check</b> - <i>проверка баз данных</i> \n"
                f"<b>/all_users</b> - <i>список всех пользователей</i> \n"
                f"<b>/subs</b> - <i>список подписчиков на рассылку</i> \n"
                f"<b>/off</b> - <i>выключить бота</i> \n"
                f"<b>/update_all</b> - <i>обновить все таблицы</i> \n"
                f"<b>/message_subs</b> - <i>рассылка прогноза погоды</i>")
        await bot.send_message(chat_id=s.admin_id,
                               text=text,
                               parse_mode='HTML')
    else:
        await bot.send_message(chat_id=user_id,
                               text=f'Данная команда вам недоступна!')


# ADMIN
@dp.message_handler(commands=["update"])
async def update_datasets(message: types.Message):
    user_id = message.from_user.id
    if user_id == s.admin_id:
        text = message.text.split()
        if len(text) == 3:
            if text[1] in SET_CITIES and text[2] in SET_TYPES:
                await update_dataset(TRANSLATE_CITIES[text[1]], text[2])
            else:
                await bot.send_message(chat_id=s.admin_id,
                                       text=f'Доступные города:\n'
                                            f'{SET_CITIES}\n'
                                            f'Доступные типы:\n'
                                            f'{SET_TYPES}\n')
        else:
            await bot.send_message(chat_id=s.admin_id,
                                   text=f'Формат ввода: /update city type')
    else:
        await bot.send_message(chat_id=user_id,
                               text=f'Данная команда вам недоступна!')


# ADMIN
@dp.message_handler(commands=["check"])
async def check_datasets(message: types.Message):
    user_id = message.from_user.id
    if user_id == s.admin_id:
        text = ""
        for type in SET_TYPES:
            for city in SET_CITIES:
                text += f"{len(lib.view(TRANSLATE_CITIES[city], type, connection, key='all'))} {city} {type} \n"
        await bot.send_message(chat_id=s.admin_id,
                               text=text)
    else:
        await bot.send_message(chat_id=user_id,
                               text=f'Данная команда вам недоступна!')


# ADMIN
@dp.message_handler(commands=["all_users"])
async def database_all_users(message: types.Message):
    user_id = message.from_user.id

    if user_id == s.admin_id:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            select_sub = f"SELECT * FROM prom.all_users;"
            cursor.execute(select_sub)
            rows = cursor.fetchall()
        names = ['id', 'username', 'date']
        text = f"{str(names[0]):^25} {str(names[1]):^25} {str(names[2]):^15} \n"

        for row in rows:
            text += f"{str(row['id']):^10} {str(row['username']):^15} {str(row['date']):<15} \n"

        await bot.send_message(chat_id=s.admin_id,
                               text=text)
    else:
        await bot.send_message(chat_id=user_id,
                               text=f'Данная команда вам недоступна!')


# ADMIN
@dp.message_handler(commands=["subs"])
async def database_subs(message: types.Message):
    user_id = message.from_user.id

    if user_id == s.admin_id:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            select_sub = f"SELECT * FROM prom.subscribers;"
            cursor.execute(select_sub)
            rows = cursor.fetchall()
        names = ['id', 'date', 'city']
        text = f"{str(names[0]):^25} {str(names[1]):^25} {str(names[2]):^15} \n"

        for row in rows:
            text += f"{str(row['id']):^10} {str(row['date']):^15} {str(row['city']):<15} \n"

        await bot.send_message(chat_id=s.admin_id,
                               text=text)
    else:
        await bot.send_message(chat_id=user_id,
                               text=f'Данная команда вам недоступна!')


# ADMIN
@dp.message_handler(commands=["off"])
async def off_bot(message: types.Message):
    user_id = message.from_user.id
    if user_id == s.admin_id:
        await bot.send_message(s.log_id, text=f"🤖 <b>выключен</b>!", parse_mode='HTML')
        connection.close()
        print(f"{datetime.now()} Бот выключен!")
        sys.exit(0)
    else:
        await bot.send_message(chat_id=user_id,
                               text=f'Данная команда вам недоступна!')


# ADMIN
@dp.message_handler(commands=["update_all"])
async def update_all_datasets(message: types.Message):
    user_id = message.from_user.id
    if user_id == s.admin_id:
        await update_dataset()
    else:
        await bot.send_message(chat_id=user_id,
                               text=f'Данная команда вам недоступна!')


# ADMIN
@dp.message_handler(commands=["message_subs"])
async def message_for_subs(message: types.Message):
    user_id = message.from_user.id
    if user_id == s.admin_id:
        await scheduled_notification()
    else:
        await bot.send_message(chat_id=user_id,
                               text=f'Данная команда вам недоступна!')


@dp.message_handler()
async def check_message(message: types.Message):
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


@dp.callback_query_handler()
async def callback_message(callback: types.CallbackQuery):
    await callback.message.delete_reply_markup()
    city, dist = callback.data.split()
    await bot.send_message(callback.from_user.id, text=f'Прогноз в городе {city} на {dist} дней:')
    await bot.send_message(callback.from_user.id, text=lib.create_forecast(city, dist, 10, connection), parse_mode='HTML')


if __name__ == "__main__":
    start_scheduler_async()
    executor.start_polling(dp, on_startup=on_startup, skip_updates=True, on_shutdown=on_shutdown)
