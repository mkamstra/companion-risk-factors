# Companion Risk Factors docker image

This folder contains a docker container specification with companion-risk-factors ready to run.

Here is what it needs to be done to generate your image:

0) Download the spark file with

`$ wget http://d3kbcqa49mib13.cloudfront.net/spark-1.6.2-bin-hadoop2.6.tgz`

and put in the companion_rf folder. The file has to be names exactly as above. If you download a newer version you have to modify the scripts accordingly (the Dockerfile and the install.sh).

1) Compile companion-risk-factor and create a .jar package (see the main README.md).
2) Copy the CompanionWeatherTraffic-0.1-jar-with-dependencies.jar to the companion_rf subfolder.
3) Execute `./build.sh` (you have to have docker installed in your machine).
4) You now have an image in your local docker repo.

If you want to run a container with this image execute the `run.sh` script. This script assumes certain locations for the volumes this container uses. There are 3 such volumes: kml, ndw, hdf. Repectively: for the kml output, dor the ndw input data and for the hdf output. The script will create and start a container that will be running as daemon. You can then issues commands with `docker exec`:

Ex: `docker exec -it companion_rf bash run_companion.sh proc <date1> <date2>`

Again see the main README.md for the list of commands.

NOTE: This container needs a running companion_db database. Either as container or locally. The condition is that the such a postgis database with name `companion` has to be acessible locally. Credentials are specified in the companion.properties file inside the container. To edit it execute:

`docker exec -it companion_rf bash`

to obtain a shell and edit the file inside.
