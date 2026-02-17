#!/bin/bash

echo "docker-compose --env-file ./app/secret/secret.env down --rmi local --volumes"
docker-compose --env-file ./app/secret/secret.env down --rmi local --volumes

echo "docker-compose --env-file ./app/secret/secret.env build"
docker-compose --env-file ./app/secret/secret.env build

echo "docker-compose --env-file ./app/secret/secret.env --profile init up airflow-init"
docker-compose --env-file ./app/secret/secret.env --profile init up airflow-init

echo "docker-compose --env-file ./app/secret/secret.env --profile debug run --rm airflow-cli"
docker-compose --env-file ./app/secret/secret.env --profile debug run --rm airflow-cli

echo "docker-compose --env-file ./app/secret/secret.env up"
docker-compose --env-file ./app/secret/secret.env up