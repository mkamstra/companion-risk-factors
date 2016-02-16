echo 'Running CompanionRiskFactors with' $# 'arguments:' $1 $2 $3

/usr/local/spark-1.6.0-bin-hadoop2.6/bin/spark-submit --driver-memory 2g --class 'no.stcorp.com.companion.CompanionRiskFactors' \
--master local[*] target/CompanionWeatherTraffic-0.1-jar-with-dependencies.jar -$1 $2,$3