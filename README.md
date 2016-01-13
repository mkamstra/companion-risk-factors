# companion-risk-factors

A prototype to determine the effect of weather and change detections to traffic using Apache Spark. This requires Apache Spark to be installed. See CompanionRiskFactors.java for more information. It has been tested to work on Ubuntu 15.04. 

The prototype has been designed to work for the Netherlands due to the availability of high resolution traffic data as well as weather data at no costs.

The weather data is downloaded from the KNMI website which can provide historical weather on an hourly basis for a large number of parameters, such as temperature, precipitation, wind direction, wind speed, etc... It can be downloaded until the last hour of the previous day. 

The traffic data is downloaded from the NDW website, which provides actual measurements as well as historic measurements. These historic measurements are for free as well. The historic measurements can be downloaded upon requests (cannot be automated as it requires some captcha image to be read and filled) and contain data for each minute in the requested interval. Both travel time, ... are available.  
but the process is rather slow as requests for data have to be made. Therefore these historic data have been downloaded locally, and the actual observations do get added to them to build up an archive of these data without the need for explicit requests. 

The processing flow will link the weather and traffic data to traffic measurement points. Subsequently the machine learning can be used to learn from these data.

Run the program as follows:

/home/osboxes/Tools/spark-1.5.1/bin/spark-submit --driver-memory 2g --class "no.stcorp.com.companion.CompanionRiskFactors" --jars /home/osboxes/.m2/repository/org/postgresql/postgresql/9.4-1206-jdbc42/postgresql-9.4-1206-jdbc42.jar,/home/osboxes/.m2/repository/org/apache/httpcomponents/httpclient/4.5.1/httpclient-4.5.1.jar,/home/osboxes/.m2/repository/org/apache/httpcomponents/httpcore/4.4.4/httpcore-4.4.4.jar,/home/osboxes/.m2/repository/commons-cli/commons-cli/1.3.1/commons-cli-1.3.1.jar --master local[*] target/CompanionWeatherTraffic-0.1.jar -proc

(replace the paths accordingly)