# b2b-maui

## Requirements:

- docker 20.10.
- docker-compose 1.29
- docker hub user

## local deploy

- docker-compose up -d --build
- docker ps to see container 

### enter Container

- docker-compose exec b2b-maui bash
- python dispatcher.py --tracker maui-oc-lapolar. // ejemplo falabella

## deploy image to docker hub

- docker login -u {USER_DOCKER} -p {PASSOWR_DOCKER}
- docker build -t b2b-maui .
- docker tag b2b-maui {USER_DOCKER}/b2b-maui:version  --example 0.1.1
- docker push {USER_DOCKER}/b2b-maui:0.1.0
- Enter in the docker-compose file in the server machine
- change docker image in the docker-compose
- docker-compose pull b2b-maui
- docker-compose up -d --build b2b-maui

