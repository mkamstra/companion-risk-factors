#!/usr/bin/env bash
echo 'Running CompanionRiskFactors with' $# 'arguments:' $1 $2 $3

export SPARK_JAVA_OPTS=-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005

/root/spark/bin/spark-submit --driver-memory 2g --class 'no.stcorp.com.companion.CompanionRiskFactors' \
--master local[*] ./CompanionWeatherTraffic-0.1-jar-with-dependencies.jar -$1 $2,$3
