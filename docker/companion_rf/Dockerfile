#FROM ubuntu:14.04
FROM mcpayment/ubuntu1404-java8

CMD mkdir /root/ndw/ /root/hdf/ /root/kml/
VOLUME /root/ndw /root/hdf /root/kml

COPY spark-1.6.2-bin-hadoop2.6.tgz /root/
COPY CompanionWeatherTraffic-0.1-jar-with-dependencies.jar /root/
COPY run_companion.sh /root/
COPY install.sh /root/
COPY companion.properties /root/

WORKDIR /root/
RUN apt-get update
RUN apt-get -qy install software-properties-common
RUN /root/install.sh

ENTRYPOINT exec top -b
