#!/bin/bash

echo "docker-compose down"
docker-compose down

echo "docker rmi weather-forecast-bot-telegram_bot"
docker rmi weather-forecast-bot-telegram_bot

echo "docker-compose --env-file ./app/secret/secret.env up"
docker-compose --env-file ./app/secret/secret.env up