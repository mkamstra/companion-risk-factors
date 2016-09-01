FROM ubuntu:15.04

# Install apt-add-repository tool
RUN apt-get update && apt-get install -y \
    software-properties-common

# Install packages
RUN apt-add-repository ppa:webupd8team/java
RUN apt-get update && apt-get install -y \
    openjdk-8-jre openjdk-8-jdk maven \
    hdf5-tools h5utils \
    wget git sudo

# Install Apache Spark
RUN wget http://www.scala-lang.org/files/archive/scala-2.10.4.tgz
RUN mkdir /usr/local/src/scala
RUN tar -xvf scala-2.10.4.tgz -C /usr/local/src/scala/
ENV SCALA_HOME=/usr/local/src/scala/scala-2.10.4
ENV PATH=$SCALA_HOME/bin:$PATH
RUN wget http://d3kbcqa49mib13.cloudfront.net/spark-1.6.0.tgz
RUN tar -xvf spark-1.6.0.tgz && cd spark-1.6.0 && build/sbt assembly

# Install OpenBLAS
COPY installObenBLASUbuntu1510.sh /tmp/
RUN cd /tmp && chmod +x installObenBLASUbuntu1510.sh && ./installObenBLASUbuntu1510.sh


# Create a workdir
RUN mkdir /home/worker
WORKDIR /home/worker

# Copy source files
COPY src/ /home/worker/src
COPY pom.xml /home/worker
COPY run_companion.sh /home/worker
COPY database.sql /home/worker
COPY companion.properties /home/worker
COPY MLInputGenerator.java /home/worker

# Build project
RUN mvn package

# Set entry
ENTRYPOINT ["./run_companion.sh"]

