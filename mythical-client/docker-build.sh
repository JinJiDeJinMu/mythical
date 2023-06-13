#!/bin/sh
#mvn clean package -DskipTests
#docker build -t xingyun-m1:latest .
## mac M系统本地构建指定了platform还是不生效，需要放到服务器上去build
docker build --platform linux/amd64 -t mythical-client:latest .
#docker login 192.168.216.190:80
#admin  Harbor12345
docker tag mythical-client:latest 192.168.217.140:18000/jinmu/mythical-client:v1.0
docker push 192.168.217.140:18000/jinmu/mythical-client:v1.0
docker run -p 18080:8080 -d 192.168.217.140:18000/jinmu/mythical-client:v1.0
docker ps