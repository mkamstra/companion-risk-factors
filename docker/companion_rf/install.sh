#!/usr/bin/env bash

cd /root

#add-apt-repository ppa:webupd8team/java -y
#apt-get update
#debconf shared/accepted-oracle-license-v1-1 select true | debconf-set-selections
#debconf shared/accepted-oracle-license-v1-1 seen true | debconf-set-selections
#sudo apt-get -y install oracle-java8-installer
#sudo apt-get -y install oracle-java8-set-default

#wget http://d3kbcqa49mib13.cloudfront.net/spark-1.6.2-bin-hadoop2.6.tgz
tar xzf spark-1.6.2-bin-hadoop2.6.tgz
ln -s spark-1.6.2-bin-hadoop2.6 spark
