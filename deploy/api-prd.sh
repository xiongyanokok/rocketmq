#!/usr/bin/env bash
CONTAINER_NAME=rocketmq-webapi
array=("10.4.63.105" "10.4.63.106")
for data in ${array[@]}
do
    DOCKER_CMD="docker --host=${data}:2375 "
    WAR_SOURCE=../${CI_PROJECT_ID}${CI_BUILD_REF_NAME}/${CONTAINER_NAME}.war
    echo "WAR_SOURCE=${WAR_SOURCE}"
    echo "REMOVING ${data}"
    ${DOCKER_CMD} ps -a|grep ${CONTAINER_NAME} |grep -v grep|awk '{print $1}'|xargs -i -t ${DOCKER_CMD} rm -f {}
    echo "STARTING ${data}"
    ${DOCKER_CMD} run \
           --env SERVER_IP=${data} \
           --volume /opt/docker/${CONTAINER_NAME}/tomcat/logs:/usr/local/tomcat/logs \
           --name ${CONTAINER_NAME} \
           --publish 8371:8080/tcp \
           --expose 8080/tcp \
           --restart always \
           --detach \
           docker-registry.hexun.com/hexunzq/tomcat8:jdk8-pinpoint
    echo "COPYING ${data}"
    ${DOCKER_CMD} cp ${WAR_SOURCE} ${CONTAINER_NAME}:/usr/local/tomcat/webapps/ROOT.war
done




