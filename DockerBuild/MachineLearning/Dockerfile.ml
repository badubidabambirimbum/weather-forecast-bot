FROM python:3.11

WORKDIR /app/machine_learning

COPY DockerBuild/MachineLearning/requirements.txt .
RUN python -m pip install --upgrade pip && pip install -r requirements.txt
RUN rm requirements.txt

ENV PYTHONUNBUFFERED=1
ENV TZ=Europe/Moscow
ENV PYTHONPATH=/app

COPY app/src/machine_learning /app/machine_learning
COPY app/src/core /app/core
