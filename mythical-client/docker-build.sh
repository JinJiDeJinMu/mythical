#!/bin/sh
#mvn clean package -DskipTests
#docker build -t xingyun-m1:latest .
docker build -t mythical-client:latest .
#docker login 192.168.216.190:80
#admin  Harbor12345
docker tag mythical-client:latest 192.168.216.190:80/jinmu/mythical-client:v1.0
docker push 192.168.216.190:80/jinmu/mythical-client:v1.0
docker run -p 18080:8080 -d 192.168.216.190:80/jinmu/mythical-client:v1.0
docker ps