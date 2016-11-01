/opt/spark/spark-1.6.2-bin-hadoop2.6/bin/spark-submit \
--class com.citic.guoan.dvb.DemandByMonth \
--master yarn-client \
/opt/jars/dvb-0.0.1-SNAPSHOT.jar \
hdfs://10.91.33.17:9000/user/hadoop/zxga_data/demodata \
hdfs://10.91.33.17:9000/user/hadoop/zxga_data/show_dict.txt \
hdfs://10.91.33.17:9000/user/hadoop/zxga_data/demand_prepare_sum.txt \
hdfs://10.91.33.17:9000/user/hadoop/zxga_data/result \
201604 \
201609 \
1000
