/home/osboxes/Tools/spark-1.5.1/bin/spark-submit --driver-memory 2g --class "SimpleApp" --jars /home/osboxes/.m2/repository/org/postgresql/postgresql/9.4-1206-jdbc42/postgresql-9.4-1206-jdbc42.jar,/home/osboxes/.m2/repository/org/apache/httpcomponents/httpclient/4.5.1/httpclient-4.5.1.jar,/home/osboxes/.m2/repository/org/apache/httpcomponents/httpcore/4.4.4/httpcore-4.4.4.jar --master local[*] target/CompanionWeatherTraffic-0.1.jar > testje.txt