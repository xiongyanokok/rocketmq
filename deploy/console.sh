#!/usr/bin/env bash

#!/usr/bin/env bash
CONTAINER_NAME=rocketmq-console
array=("10.4.63.103")
for data in ${array[@]}
do
    DOCKER_CMD="docker --host=${data}:2375 "
    JAR_SOURCE=../${CI_PROJECT_ID}${CI_BUILD_REF_NAME}/rocketmq-console-ng-1.0.0.jar
    echo "JAR_SOURCE=${JAR_SOURCE}"
    echo "REMOVING ${data}"
    ${DOCKER_CMD} ps -a|grep ${CONTAINER_NAME} |grep -v grep|awk '{print $1}'|xargs -i -t ${DOCKER_CMD} rm -f {}
    echo "STARTING ${data}"
    ${DOCKER_CMD} run -i -d \
           --volume /opt/docker/${CONTAINER_NAME}/tomcat/logs:/tmp \
           --name ${CONTAINER_NAME} \
           --publish 8080:8080/tcp \
           --expose 8080/tcp \
           --restart always \
           java:8 \
           tail -f -n20 /etc/hosts

    echo "COPYING ${data}"
    ${DOCKER_CMD} cp ${JAR_SOURCE} ${CONTAINER_NAME}:/app.jar

    echo "run console"
    ${DOCKER_CMD} exec rocketmq-console sh -c "nohup java $JAVA_OPTS -jar /app.jar &"

done