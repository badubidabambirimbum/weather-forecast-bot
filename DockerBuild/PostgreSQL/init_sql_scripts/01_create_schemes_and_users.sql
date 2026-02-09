CREATE SCHEMA prom;
CREATE SCHEMA backup;
CREATE SCHEMA metrics;
CREATE SCHEMA predict;

CREATE USER airflow WITH PASSWORD 'password_airflow';
CREATE USER username WITH PASSWORD 'password_username';

CREATE DATABASE airflow_db OWNER airflow;

GRANT ALL PRIVILEGES ON DATABASE airflow_db TO airflow;
GRANT ALL PRIVILEGES ON DATABASE telegram_bot_db TO username;
GRANT ALL PRIVILEGES ON DATABASE airflow_db TO username;