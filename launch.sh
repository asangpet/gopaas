#!/bin/bash
curl -X POST -H "Content-Type: application/json" -d '{"name":"ak"}' http://localhost:3000/cluster
curl -X POST -H "Content-Type: application/json" -d '{"name":"ak"}' http://localhost:3000/grow
curl -X POST -H "Content-Type: application/json" -d '{"name":"ak"}' http://localhost:3000/grow
docker ps | grep mongo | cut -d' ' -f1 | xargs docker stop
docker ps -a | grep mongo | cut -d' ' -f1 | xargs docker rm

