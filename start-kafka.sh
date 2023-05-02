#!/bin/bash
sudo docker compose up -d --remove-orphans && sudo docker exec -it waze-monitorer-kafka-1-1 kafka-topics --create --bootstrap-server localhost:29092 --topic JAM && sudo docker exec -it waze-monitorer-kafka-1-1 kafka-topics --create --bootstrap-server localhost:29092 --topic HAZARD
