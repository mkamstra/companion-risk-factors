# companion-risk-factors

A prototype to determine the effect of weather and change detections to traffic using Apache Spark. This requires Apache Spark to be installed. It has been tested to work on Ubuntu 15.04, but should be platform independent. If not please report so.

Build it using Maven (typically mvn package on the directory where this readme file is located). Note that you might need to change a line in the pom.xml file: <jhdf5.native>linux</jhdf5.native>  Set this to the platform that is applicable.

The prototype has been designed to work for the Netherlands due to the availability of high resolution traffic data as well as observed weather data at no costs.

The weather data is downloaded from the KNMI website which can provide historical weather on an hourly basis for a large number of parameters, such as temperature, precipitation, wind direction, wind speed, etc... It can be downloaded for free until the last hour of the previous day.

The traffic data is downloaded from the NDW website, which provides actual measurements as well as historic measurements. These historic measurements are for free as well. The historic measurements can be downloaded upon requests (cannot be automated as it requires some captcha image to be read and filled) and contain data for each minute in the requested interval. Both travel time and traffic speed are available.  
Unfortunately the process is rather slow as requests for data have to be made, which will be available for download minutes to several hours later depending on the size of the requested package. Therefore these historic data have been downloaded locally (to our Snufkin shared folder). The actual observations do get added to them to build up an archive of these data without the need for explicit requests when running this software. The locally downloaded software can be accessed by an FTP server. The data request can be made [here](ndw.numberOfTrafficSpeedFilesPerBatch=200).

The processing flow will link the weather and traffic data to traffic measurement points.

To be able to inspect the relations between the data the plot option is available. Data that has been previously generated will be saved to BOS files that basically contain the chart data for time series, but can very well serve as input data for machine learning as well, since these data contain the correlated traffic and weather data. The plotting option allows for merging multiple files of the same measurement site (NDW ids) to be able inspect longer intervals (as generating the data takes quite long the chart data usually don't contain more than just one day).

The HDF5 export functionality uses JNI (Java Native Interface) libraries. Therefor, to compile this program you must select which platform's JNI library to use. You configure this by commenting out the appropriate jhdf5.native tags in the pom.xml file. After that you run `mvn package` as usual for Maven projects.

Run the program as follows (from the root of the software, ie. where you find this README.md file):

* `./run_companion.sh tcm` (add traffic measurement sites to the database if they are not there already; it is recommended to do this first when you run the software for the first time)

* `./run_companion.sh wo` (get weather observations and add weater measurement sites to the database if they are not there already; it is recommended to do this second when you run the software for the first time)

* `./run_companion.sh link` (link weather and traffic stations to each other; needed before being able to process; recommended to do after tcm and wo)

* `./run_companion.sh proc 2015-12-02-08 2015-12-02-09` (process to link weather and traffic data to each other)

* `./run_companion.sh export 2015-12-02-08 2015-12-02-09` (like `proc` but exports to HDF5 files)

* `./run_companion.sh ts` (get traffic speed measurements)

* `./run_companion.sh plot`  (plot BOS files: a window will pop up to select the desired files to plot - the names of the files should be rather self explanatory)

* `./run_companion.sh kml` (generate a set of kml files regarding the weather and traffic measurement sites)


Note that you can configure the software for your system using the companion.properties file. An example of the contents of this file:
> `ndw.ftp.user=companion`<br>
> `ndw.ftp.password=******** (hidden for obvious reasons)`<br>
> `ndw.ftp.url=192.168.1.33`<br>
> `ndw.ftp.folder=/Projects/companion/downloadedData/NDW/`<br>
> `ndw.localFolder=//usr//local//data//ndw//`<br>
> `ndw.hdfFolder=/Volumes/CompanionEx/Data/hdf/` <br>
> `ndw.kmlFolder=/Volumes/CompanionEx/Data/kml/` <br>`
> `ndw.useLocalData=true`<br>
> `ndw.db.user=snt`<br>
> `ndw.db.password=*********`<br>
> `ndw.db.host=192.168.99.100`<br>
> `ndw.db.port=32768`<br>
> `ndw.numberOfTrafficSpeedFilesPerBatch=200` <br>

Note that Spark can generate an insane amount of input making it hard to find your own log messages back. To reduce this you can find the log4j.properties file in the conf directory here and copy it to the conf directory of your Spark installation (e.g. /usr/local/spark-1.6.0-bin-hadoop2.6/conf/)
