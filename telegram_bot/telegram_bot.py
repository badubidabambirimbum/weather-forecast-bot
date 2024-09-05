from aiogram import Bot, types, Dispatcher, executor
from auth_data import token  # API KEY
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton, \
    ReplyKeyboardRemove
from Keyboards import kb, kb_help, kb_cities, ikb_info
import pandas as pd

import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from parsing.table import table

HELP_COMMAND = """
<b>/start</b> - начать работу с ботом
<b>/help</b> - как пользоваться ботом
<b>/info</b> - информация о проекте
<b>/cities</b> - доступные города
<b>/weather</b> - обозначения погоды"""

WEATHER_YANDEX_SMILE = {'Гроза': "🌩",
                        'Ливень': "🌧",
                        'Дождь': "💦",
                        'Облачно с прояснениями': '⛅',
                        'Дождь с грозой': '⛈',
                        'Ясно': '☀',
                        'Пасмурно': '☁',
                        'Малооблачно': "🌤",
                        'Небольшой дождь': "💧"}

WEATHER_GISMETEO_SMILE = {'Безоблачно': "☀",
                          'Гроза': "🌩",
                          'Дождь': "💦",
                          'Ливень': "🌧",
                          'Малооблачно': "🌤",
                          'Малооблачно, дождь': "💦",
                          'Малооблачно, дождь, гроза': "⛈",
                          'Малооблачно, без осадков': "🌤",
                          'Малооблачно, небольшой  дождь': "💧",
                          'Малооблачно, небольшой  дождь, гроза': "💧⚡️",
                          'Небольшой дождь': "💧",
                          'Облачно с прояснениями': "⛅",
                          'Облачно, дождь': "🌥💦",
                          'Облачно, дождь, гроза': "🌥💦️⚡️",
                          'Облачно, без осадков': "🌥",
                          'Облачно, небольшой дождь': "🌥💧",
                          'Облачно, сильный  дождь': "🌧",
                          'Облачно, сильный дождь, гроза': "⛈",
                          'Пасмурно': "☁",
                          'Пасмурно, дождь, гроза': "☁💦⚡️",
                          'Пасмурно, сильный  дождь': "🌧",
                          'Пасмурно, сильный  дождь, гроза': "⛈",
                          'Ясно': "☀"}

SET_CITIES = {"Москва", "Екатеринбург", "Краснодар"}

TRANSLATE_CITIES = {"Москва": "Moscow",
                    "Екатеринбург": "Ekaterinburg",
                    "Краснодар": "Krasnodar"}

bot = Bot(token)
dp = Dispatcher(bot)
table = table()


async def on_startup(_):
    print("Бот был успешно запущен!")


async def on_shutdown(_):
    print("Бот выключен!")


@dp.message_handler(commands=["start"])
async def start_message(message: types.Message):
    await bot.send_sticker(message.from_user.id,
                           sticker="CAACAgIAAxkBAAEMj01mp68a2RxE2V-27EZhT1TxljV3zQACjRAAAl_bkUp3Bt1MNp18SzUE",
                           reply_markup=kb_help)
    await message.answer(
        'Привет!👋 \nЗдесь ты найдешь ближайший прогноз погоды 🌦 в интересующем тебя городе! \nДля дополнительной информации воспользуйся командой /help')


@dp.message_handler(commands=["help"])
async def help_message(message: types.Message):
    await bot.send_sticker(message.from_user.id,
                           sticker="CAACAgIAAxkBAAEMj1Fmp6-tcw1DpXSWJp3yCkcgTFAy6QACshIAAmD9iUtRNBJT06z1kDUE",
                           reply_markup=ReplyKeyboardRemove())
    await message.reply(text='Введи название города🏙 и выбери количество дней📅 для получения прогноза в этом городе на выбранный период!😉', parse_mode='HTML', reply_markup=kb_cities)


@dp.message_handler(commands=["info"])
async def info_message(message: types.Message):
    await bot.send_sticker(message.from_user.id,
                           sticker="CAACAgEAAxkBAAEMrDFmxDk66eDeDYk0jqiSZvGBeX2klAAC0gMAAuJ5IET7lNR5d0OiyjUE")
    await bot.send_message(chat_id=message.from_user.id,
                           text='Ниже представлены ссылка на GitHub проекта📄 и на мой Telegram для обратной связи❤️',
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
        mes_gis += weather + " " + WEATHER_GISMETEO_SMILE[weather] + "\n"
    await bot.send_sticker(message.from_user.id,
                           sticker="CAACAgEAAxkBAAEMj_RmqKuKC9rmnTElJX3QEr-MYpC-cAACXQMAApzteUVTI9qtaJq7kTUE",
                           reply_markup=kb_cities)
    await message.answer(text=mes_ya)
    await message.answer(text=mes_gis)


@dp.message_handler(commands=["add"])
async def add_mesage(message: types.Message):
    command, *args = message.text.split()

    if args:
        word = ' '.join(args)
    else:
        await message.reply("Пожалуйста, укажите слово после команды /add.")


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
        await message.reply('Я вас не понимаю 😔 \nВоспользуйтесь, пожалуйста, командой /help')


@dp.callback_query_handler()
async def callback_message(callback: types.CallbackQuery):
    await callback.message.delete_reply_markup()

    city, dist = callback.data.split()

    await bot.send_message(callback.from_user.id, f'Прогноз в городе {city} на {dist} дней:')

    forecast_Yandex = table.datasets[TRANSLATE_CITIES[city]]["Yandex"].iloc[-1]
    forecast_GisMeteo = table.datasets[TRANSLATE_CITIES[city]]["GisMeteo"].iloc[-1]
    date_forecast = table.datasets[TRANSLATE_CITIES[city]]["Yandex"].index[-1]

    future_dates = pd.date_range(start=date_forecast, periods=10)
    forecast_data = ""

    for i in range(1, int(dist) + 1):
        date = future_dates[i - 1]
        if forecast_Yandex[f'weather{i}'] in WEATHER_YANDEX_SMILE and forecast_GisMeteo[f'weather{i}'] in WEATHER_GISMETEO_SMILE:
            if WEATHER_YANDEX_SMILE[forecast_Yandex[f'weather{i}']] == WEATHER_GISMETEO_SMILE[forecast_GisMeteo[f'weather{i}']]:
                forecast_data += (f"\n"
                                  f"✨ {date.strftime('%Y-%m-%d')} ✨\n"
                                  f"<b>Температура</b> от <b>{str(forecast_Yandex[f'night{i}'])}</b> до <b>{str(forecast_Yandex[f'day{i}'])}</b>\n 🔸<b>Yandex</b> и 🔹<b>GisMeteo</b> прогнозируют {WEATHER_GISMETEO_SMILE[forecast_GisMeteo[f'weather{i}']]}\n")
            else:
                forecast_data += (f"\n"
                                  f"✨ {date.strftime('%Y-%m-%d')} ✨\n"
                                  f"<b>Температура</b> от <b>{str(forecast_Yandex[f'night{i}'])}</b> до <b>{str(forecast_Yandex[f'day{i}'])}</b>\n 🔸<b>Yandex</b> прогнозирует {WEATHER_YANDEX_SMILE[forecast_Yandex[f'weather{i}']]}\n 🔹<b>GisMeteo</b> прогнозирует {WEATHER_GISMETEO_SMILE[forecast_GisMeteo[f'weather{i}']]}\n")
        else:
            print(f"Yandex: {forecast_Yandex[f'weather{i}']}, GisMeteo: {forecast_GisMeteo[f'weather{i}']}")
            forecast_data += (f"\n"
                              f"✨ {date.strftime('%Y-%m-%d')} ✨\n"
                              f"<b>Температура</b> от <b>{str(forecast_Yandex[f'night{i}'])}</b> до <b>{str(forecast_Yandex[f'day{i}'])}</b>\n 🔸<b>Yandex</b> прогнозирует {forecast_Yandex[f'weather{i}']}\n 🔹<b>GisMeteo</b> прогнозирует {forecast_GisMeteo[f'weather{i}']}\n")

    await bot.send_message(callback.from_user.id, text=forecast_data, parse_mode='HTML')


if __name__ == "__main__":
    executor.start_polling(dp, on_startup=on_startup, skip_updates=True, on_shutdown=on_shutdown)
