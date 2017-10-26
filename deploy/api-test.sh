#!/usr/bin/env bash

CONTAINER_NAME=rocketmq-webapi
array=("10.4.63.103""10.4.63.104:9876")
for data in ${array[@]}
do
    DOCKER_CMD="docker --host=${data}:2375 "
    WAR_SOURCE=../${CI_PROJECT_ID}${CI_BUILD_REF_NAME}/${CONTAINER_NAME}.war
    echo "WAR_SOURCE=${WAR_SOURCE}"
    echo "REMOVING ${data}"
    ${DOCKER_CMD} ps -a|grep ${CONTAINER_NAME} |grep -v grep|awk '{print $1}'|xargs -i -t ${DOCKER_CMD} rm -f {}

    echo "STARTING ${data}"
    ${DOCKER_CMD} run --env SERVER_PORT=2234 \
           --env SERVER_IP=${data} \
           --env PINPOINT_AGENTID=test1 \
           --env PINPOINT_APPLICATIONNAME=rocketmq \
           --env COLLECTOR_IP=10.4.63.103 \
           --env PINPOINT_AGENT_DOWNLOAD_URL=http://test.caidao.hexun.com/pinpoint-agent.tar.gz \
           --volume /opt/docker/${CONTAINER_NAME}/tomcat/logs:/usr/local/tomcat/logs \
           --name rocketmq-webapi \
           --publish 2234:2234/tcp \
           --expose 2234/tcp \
           --expose 2234/tcp \
           --restart always \
           --detach \
           docker-registry.hexun.com/hexunzq/javaweb:7-jdk7-v4

    echo "COPYING ${data}"
    ${DOCKER_CMD} cp ${WAR_SOURCE} ${CONTAINER_NAME}:/usr/local/tomcat/webapps/ROOT.war
done




