from aiogram import Bot, types, Dispatcher, executor
from auth_data import token  # API KEY
from auth_data import admin_id, log_id  # ADMIN ID, LOG ID
from telegram_constants import *
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardRemove
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from Keyboards import kb, kb_help, kb_cities, ikb_info
from datetime import datetime
import pandas as pd
import asyncio

import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from parsing.table import table

bot = Bot(token)
dp = Dispatcher(bot)
table = table()

scheduler_async = AsyncIOScheduler()

all_users_id = pd.read_csv('users_data/all_users_id.csv')
add_users_id = pd.read_csv('users_data/add_users_id.csv')


def create_forecast(city, dist, period):
    forecast_Yandex = table.datasets[TRANSLATE_CITIES[city]]["Yandex"].iloc[-1]
    forecast_GisMeteo = table.datasets[TRANSLATE_CITIES[city]]["GisMeteo"].iloc[-1]
    date_forecast = table.datasets[TRANSLATE_CITIES[city]]["Yandex"].index[-1]

    future_dates = pd.date_range(start=date_forecast, periods=period)
    forecast_data = ""

    for i in range(1, int(dist) + 1):
        date = future_dates[i - 1]
        if (forecast_Yandex[f'weather{i}'] in WEATHER_YANDEX_SMILE and
                forecast_GisMeteo[f'weather{i}'] in WEATHER_GISMETEO_SMILE):
            if WEATHER_YANDEX_SMILE[forecast_Yandex[f'weather{i}']] == WEATHER_GISMETEO_SMILE[forecast_GisMeteo[f'weather{i}']]:
                forecast_data += (f"\n"
                                  f"✨ {date.strftime('%Y-%m-%d')} ✨\n"
                                  f"<b>Температура</b> ⬇️ <b>{str(forecast_Yandex[f'night{i}'])}°</b> ⬆️ <b>{str(forecast_Yandex[f'day{i}'])}°</b>\n"
                                  f"🔸<b>Yandex</b> и 🔹<b>GisMeteo</b> прогнозируют {WEATHER_GISMETEO_SMILE[forecast_GisMeteo[f'weather{i}']]}\n")
            else:
                forecast_data += (f"\n"
                                  f"✨ {date.strftime('%Y-%m-%d')} ✨\n"
                                  f"<b>Температура</b> ⬇️ <b>{str(forecast_Yandex[f'night{i}'])}°</b> ⬆️ <b>{str(forecast_Yandex[f'day{i}'])}°</b>\n "
                                  f"🔸<b>Yandex</b> прогнозирует {WEATHER_YANDEX_SMILE[forecast_Yandex[f'weather{i}']]}\n "
                                  f"🔹<b>GisMeteo</b> прогнозирует {WEATHER_GISMETEO_SMILE[forecast_GisMeteo[f'weather{i}']]}\n")
        else:
            forecast_data += (f"\n"
                              f"✨ {date.strftime('%Y-%m-%d')} ✨\n"
                              f"<b>Температура</b> ⬇️ <b>{str(forecast_Yandex[f'night{i}'])}°</b> ⬆️ <b>{str(forecast_Yandex[f'day{i}'])}°</b>\n "
                              f"🔸<b>Yandex</b> прогнозирует {forecast_Yandex[f'weather{i}']}\n "
                              f"🔹<b>GisMeteo</b> прогнозирует {forecast_GisMeteo[f'weather{i}']}\n")

    return forecast_data


async def add_user(city: str, message: types.Message):
    global add_users_id

    user_id = message.from_user.id
    if user_id not in add_users_id['id'].values:
        new_user = pd.DataFrame({"id": [user_id],
                                 "date": [datetime.now().strftime('%Y-%m-%d')],
                                 "city": [city]})

        add_users_id = pd.concat([add_users_id, new_user], ignore_index=True)
        add_users_id.to_csv(f'users_data/add_users_id.csv', index=False)

        print(f"Новая подписка на оповещение о погоде: {user_id}")
        await bot.send_message(chat_id=log_id,
                               text=f"✅ Новая подписка на оповещение о погоде: {user_id}",
                               parse_mode='HTML')

        await message.reply("✅ Отлично!😄\n"
                            "Каждый день в 7️⃣ утра по МСК бот🤖 будет присылать вам прогноз погоды на предстоящий день!😉")
    else:
        await message.reply("Вы уже подписаны на оповещение о погоде❗️️")


async def scheduled_notification():
    for user in add_users_id.itertuples():
        try:
            await bot.send_message(user.id, text=create_forecast(user.city, 1, 1), parse_mode='HTML')
        except Exception as e:
            print(f"Не удалось отправить сообщение пользователю {user.id}: {e}")
            await bot.send_message(chat_id=log_id,
                                   text=f"❌ Не удалось отправить сообщение пользователю {user.id}: {e}",
                                   parse_mode='HTML')


async def update_dataset(city, type):
    try:
        table.update(city, type)
        await bot.send_message(log_id, text=f"{datetime.now().date()}\n✅ {city} {type}", parse_mode='HTML')
    except Exception as e:
        print(f"Ошибка: {e}")
        await bot.send_message(log_id, text=f"❌ Ошибка: {e}", parse_mode='HTML')
        await asyncio.sleep(1800)
        try:
            table.update(city, type)
            await bot.send_message(log_id, text=f"{datetime.now().date()}\n✅ {city} {type}", parse_mode='HTML')
        except Exception as e:
            print(f"Ошибка: {e}")
            await bot.send_message(log_id, text=f"❌ Ошибка: {e}", parse_mode='HTML')


async def backup_dataset():
    try:
        table.backup()
        await bot.send_message(log_id, text=f"{datetime.now().date()}\n✅ BACKUP", parse_mode='HTML')
    except Exception as e:
        await bot.send_message(log_id, text=f"{datetime.now().date()}\n❌ BACKUP {e}", parse_mode='HTML')


def start_scheduler_async():
    scheduler_async.add_job(scheduled_notification, 'cron', hour=7, minute=0)
    scheduler_async.add_job(backup_dataset, 'cron', hour=4, minute=50)
    scheduler_async.add_job(update_dataset, 'cron', hour=5, minute=0, args=["Moscow", "GisMeteo"])
    scheduler_async.add_job(update_dataset, 'cron', hour=4, minute=57, args=["Krasnodar", "Yandex"])
    scheduler_async.add_job(update_dataset, 'cron', hour=5, minute=19, args=["Ekaterinburg", "GisMeteo"])
    scheduler_async.add_job(update_dataset, 'cron', hour=5, minute=27, args=["Moscow", "Yandex"])
    scheduler_async.add_job(update_dataset, 'cron', hour=5, minute=21, args=["Krasnodar", "GisMeteo"])
    scheduler_async.add_job(update_dataset, 'cron', hour=5, minute=51, args=["Ekaterinburg", "Yandex"])
    scheduler_async.start()


async def on_startup(_):
    await bot.send_message(log_id, text=f"🤖 <b>запущен</b>!", parse_mode='HTML')
    print(f"{datetime.now()} Бот был успешно запущен!")


async def on_shutdown(_):
    await bot.send_message(log_id, text=f"🤖 <b>выключен</b>!", parse_mode='HTML')
    print(f"{datetime.now()} Бот выключен!")


@dp.message_handler(commands=["start"])
async def start_message(message: types.Message):
    global all_users_id
    user_id = message.from_user.id

    if user_id not in all_users_id['id'].values:
        new_user = pd.DataFrame({"id": [user_id],
                                 "date": [datetime.now().strftime('%Y-%m-%d')],
                                 "username": [message.from_user.username]})

        all_users_id = pd.concat([all_users_id, new_user], ignore_index=True)
        all_users_id.to_csv(f'users_data/all_users_id.csv', index=False)

        print(f"Новый пользователь: {message.from_user.username}!!!")
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
    await add_user(message.text[1:].lower(), message)


@dp.message_handler(lambda message: '+краснодар' == message.text.lower())
async def add_Krasnodar(message: types.Message):
    await add_user(message.text[1:].lower(), message)


@dp.message_handler(lambda message: '+екатеринбург' == message.text.lower())
async def add_Ekaterinburg(message: types.Message):
    await add_user(message.text[1:].lower(), message)


@dp.message_handler(commands=["remove"])
async def remove_message(message: types.Message):
    global add_users_id
    user_id = message.from_user.id

    if user_id in add_users_id['id'].values:
        index_to_remove = add_users_id[add_users_id['id'] == user_id].index

        add_users_id.drop(index_to_remove, inplace=True)
        add_users_id.reset_index(drop=True, inplace=True)
        add_users_id.to_csv(f'users_data/add_users_id.csv', index=False)

        print(f"Отписка: {message.from_user.username}")
        await bot.send_message(chat_id=log_id,
                               text=f"❗️ Отписка: {message.from_user.username}",
                               parse_mode='HTML')

        await message.reply("Вы успешно отписались от оповещения о погоде! ✔️")
    else:
        await message.reply("Вы не были подписаны на оповещение о погоде❗️")


# ADMIN
@dp.message_handler(commands=["admin"])
async def admin_list(message: types.Message):
    user_id = message.from_user.id
    if str(user_id) == admin_id:
        text = (f"<b>/update</b> - <i>обновление базы данных</i> \n"
                f"<b>/check</b> - <i>проверка баз данных</i> \n")
        await bot.send_message(chat_id=user_id,
                               text=text,
                               parse_mode='HTML')
    else:
        await bot.send_message(chat_id=user_id,
                               text=f'Данная команда вам недоступна!')


# ADMIN
@dp.message_handler(commands=["update"])
async def update_datasets(message: types.Message):
    user_id = message.from_user.id
    if str(user_id) == admin_id:
        text = message.text.split()
        if len(text) == 3:
            if text[1] in SET_CITIES and text[2] in SET_TYPES:
                await update_dataset(TRANSLATE_CITIES[text[1]], text[2])
            else:
                await bot.send_message(chat_id=user_id,
                                       text=f'Доступные города:\n'
                                            f'{SET_CITIES}\n'
                                            f'Доступные типы:\n'
                                            f'{SET_TYPES}\n')
        else:
            await bot.send_message(chat_id=user_id,
                                   text=f'Формат ввода: /update city type')
    else:
        await bot.send_message(chat_id=user_id,
                               text=f'Данная команда вам недоступна!')


# ADMIN
@dp.message_handler(commands=["check"])
async def check_datasets(message: types.Message):
    user_id = message.from_user.id
    if str(user_id) == admin_id:
        text = ""
        for type in SET_TYPES:
            for city in SET_CITIES:
                text += f"{len(table.datasets[TRANSLATE_CITIES[city]][type])} {city} {type} \n"
        await bot.send_message(chat_id=user_id,
                               text=text)
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
    await bot.send_message(callback.from_user.id, text=create_forecast(city, dist, 10), parse_mode='HTML')


if __name__ == "__main__":
    start_scheduler_async()
    executor.start_polling(dp, on_startup=on_startup, skip_updates=True, on_shutdown=on_shutdown)
