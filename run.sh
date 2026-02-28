#!/bin/bash

del_containers=$1
del_images=$2
build=$3
init=$4
run=$5

echo "del_containers:"$del_containers", del_image:"$del_images", build: "$build", init:"$init", run":$run

if [[ $del_containers == '1' ]]; then
  read -p "Delete containers? (y/N): " answer

  if [[ "$answer" == 'Y' || "$answer" == 'y' ]]; then
    echo "Operation started"
    docker-compose --env-file ./app/secret/secret.env down
    docker-compose --env-file ./app/secret/secret.env --profile init down
    docker-compose --env-file ./app/secret/secret.env --profile worker_ml_model down
  else
    echo "Delete containers cancelled"
  fi

fi


if [[ $del_images == '1' ]]; then
  read -p "Delete images? (y/N): " answer

  if [[ "$answer" == 'Y' || "$answer" == 'y' ]]; then
    echo "Operation started"
    docker images --format "{{.Repository}} {{.ID}}" | grep weather-forecast-bot | awk '{print ($1, $2)}' |
    while read repo id
    do
      echo "docker rmi "$repo":"$id""
      docker rmi $id
    done
  else
    echo "Delete images cancelled"
  fi

fi


if [[ $build == '1' ]]; then
echo "docker-compose --env-file ./app/secret/secret.env build"
docker-compose --env-file ./app/secret/secret.env build

echo "docker-compose --env-file ./app/secret/secret.env --profile init build"
docker-compose --env-file ./app/secret/secret.env --profile init build

echo "docker-compose --env-file ./app/secret/secret.env --profile worker_ml_model build"
docker-compose --env-file ./app/secret/secret.env --profile worker_ml_model build
fi


if [[ $init == '1' ]]; then
echo "docker compose --env-file ./app/secret/secret.env up -d database"
docker compose --env-file ./app/secret/secret.env up -d database
echo "compose --env-file ./app/secret/secret.env --profile init run --rm airflow-init"
docker compose --env-file ./app/secret/secret.env --profile init run --rm airflow-init
echo "docker-compose --env-file ./app/secret/secret.env --profile init run --rm airflow-cli"
docker-compose --env-file ./app/secret/secret.env --profile init run --rm airflow-cli
echo "docker-compose --env-file ./app/secret/secret.env stop"
docker-compose --env-file ./app/secret/secret.env stop
fi


if [[ $run == '1' ]]; then
echo "docker-compose --env-file ./app/secret/secret.env up"
docker-compose --env-file ./app/secret/secret.env up
fi