#!/usr/bin/env bash

airflow connections add 'database_connect' \
    --conn-uri 'postgresql://tg_bot:password_postgres@database:5432/telegram_bot_db'