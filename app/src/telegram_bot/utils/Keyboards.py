from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton

kb = ReplyKeyboardMarkup(resize_keyboard=True)
b1 = KeyboardButton(text="Москва")
b2 = KeyboardButton(text="Екатеринбург")
b3 = KeyboardButton(text="Краснодар")
kb.add(b1, b2, b3)

kb_help = ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
b4 = KeyboardButton(text="/help")
kb_help.add(b4)

kb_cities = ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
b5 = KeyboardButton(text="/cities")
kb_cities.add(b5)

ikb_info = InlineKeyboardMarkup(row_width=3)
ib1 = InlineKeyboardButton(text="Github", url="https://github.com/badubidabambirimbum/weather-forecast-bot")
ib2 = InlineKeyboardButton(text="telegram", url="https://t.me/terembit")
ikb_info.add(ib1, ib2)