#!/bin/bash
#check-config-server-started.sh

apt-get update -y
yes | apt-get install curl

curlResult=$(curl -s -o /dev/null -I -w "%{http_code}" http://config-server:8888/actuator/health)

echo "Config Server HTTP Status Code: $curlResult"


while [ ! "${curlResult}" -eq 200 ]; do
    >&2 echo "Config Server is not up yet. Waiting..."
    sleep 2
    curlResult=$(curl -s -o /dev/null -I -w "%{http_code}" http://config-server:8888/actuator/health)
    echo "Config Server HTTP Status Code: $curlResult"
done

./cnb/lifecycle/launcher