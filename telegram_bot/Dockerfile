FROM python:3.11

RUN apt-get update

WORKDIR /app

COPY . .

RUN python -m pip install --upgrade pip && pip install -r requirements.txt

ENV PYTHONUNBUFFERED=1
ENV TZ=Europe/Moscow

CMD ["python3", "telegram_bot.py"]
