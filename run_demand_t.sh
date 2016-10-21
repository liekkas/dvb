/opt/spark/spark-1.6.2-bin-hadoop2.6/bin/spark-submit \
--class com.citic.guoan.dvb.DemandByHour \
--master yarn-cluster \
/opt/jars/dvb-0.0.1-SNAPSHOT.jar \
hdfs://10.91.33.17:9000/user/hadoop/zxga_data/demodata \
hdfs://10.91.33.17:9000/user/hadoop/zxga_data/show_dict.txt \
hdfs://10.91.33.17:9000/user/hadoop/zxga_data/demand_uid_count \
/opt/data/result