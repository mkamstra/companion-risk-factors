/home/osboxes/Tools/spark-1.5.1/bin/spark-submit --driver-memory 2g --class "no.stcorp.com.companion.CompanionRiskFactors" --jars /home/osboxes/.m2/repository/org/postgresql/postgresql/9.4-1206-jdbc42/postgresql-9.4-1206-jdbc42.jar,/home/osboxes/.m2/repository/org/apache/httpcomponents/httpclient/4.5.1/httpclient-4.5.1.jar,/home/osboxes/.m2/repository/org/apache/httpcomponents/httpcore/4.4.4/httpcore-4.4.4.jar,/home/osboxes/.m2/repository/commons-cli/commons-cli/1.3.1/commons-cli-1.3.1.jar,/home/osboxes/.m2/repository/org/jfree/jfreechart/1.0.19/jfreechart-1.0.19.jar,/home/osboxes/.m2/repository/org/jfree/jcommon/1.0.23/jcommon-1.0.23.jar --master local[*] target/CompanionWeatherTraffic-0.1.jar -proc 2015-12-02-08,2015-12-02-09
