/opt/spark/spark-1.6.2-bin-hadoop2.6/bin/spark-submit \
--class com.citic.guoan.dvb.DemandByHour \
--master local[2] \
--jars /opt/jars/jedis-2.8.1.jar \
/opt/jars/dvb-0.0.1-SNAPSHOT.jar \
/opt/data/demand.txt \
/opt/data/show_dict.txt