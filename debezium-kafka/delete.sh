

docker rm -f $(docker ps -a -q)

docker volume rm $(docker volume ls -q)

docker compose up -d

docker exec -it ksqldb-cli ksql http://ksqldb-server:8088