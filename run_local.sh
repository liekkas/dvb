/Users/liekkas/env/bd/spark/spark-1.6.0-cdh5.7.1/bin/spark-submit \
--class com.citic.guoan.dvb.DemandByMonth \
--master local[2] \
--jars /Users/liekkas/.m2/repository/redis/clients/jedis/2.8.1/jedis-2.8.1.jar \
/Users/liekkas/IdeaProjects/asiainfo/scala/dvb/target/dvb-0.0.1-SNAPSHOT.jar \
/Users/liekkas/IdeaProjects/asiainfo/scala/dvb/data/demand.txt \
/Users/liekkas/IdeaProjects/asiainfo/scala/dvb/data/show_dict.txt