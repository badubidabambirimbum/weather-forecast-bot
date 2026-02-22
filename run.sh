#!/bin/bash

if [ $1 == 'all' ]; then
echo "docker-compose --env-file ./app/secret/secret.env down --rmi --volumes all"
docker-compose --env-file ./app/secret/secret.env down --rmi --volumes all
elif [ $1 == 'local' ]; then
echo "docker-compose --env-file ./app/secret/secret.env down --rmi --volumes local"
docker-compose --env-file ./app/secret/secret.env down --rmi --volumes local
else
echo "docker-compose --env-file ./app/secret/secret.env down --rmi local --volumes"
docker-compose --env-file ./app/secret/secret.env down --rmi local --volumes
fi

echo "docker-compose --env-file ./app/secret/secret.env build"
docker-compose --env-file ./app/secret/secret.env build

echo "docker build -t ml_fit_model:latest -f DockerBuild/MachineLearning/Dockerfile.ml ."
docker build -t ml_fit_model:latest -f DockerBuild/MachineLearning/Dockerfile.ml .

echo "docker-compose --env-file ./app/secret/secret.env --profile init up airflow-init"
docker-compose --env-file ./app/secret/secret.env --profile init up airflow-init

echo "docker-compose --env-file ./app/secret/secret.env --profile debug run --rm airflow-cli"
docker-compose --env-file ./app/secret/secret.env --profile debug run --rm airflow-cli

echo "docker-compose --env-file ./app/secret/secret.env up"
docker-compose --env-file ./app/secret/secret.env up