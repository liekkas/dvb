/Users/liekkas/env/bd/spark/spark-1.6.2-bin-hadoop2.6/bin/spark-submit \
--class com.citic.guoan.dvb.DemandByMonth \
--master local[2] \
--jars /Users/liekkas/.m2/repository/redis/clients/jedis/2.8.1/jedis-2.8.1.jar \
/Users/liekkas/IdeaProjects/asiainfo/scala/dvb/target/dvb-0.0.1-SNAPSHOT.jar \
/Users/liekkas/asiainfo/projects/中信国安广视/数据源/demanddata \
/Users/liekkas/IdeaProjects/asiainfo/scala/dvb/data/show_dict.txt \
/Users/liekkas/IdeaProjects/asiainfo/scala/dvb/data/demand_prepare_sum.txt \
/Users/liekkas/IdeaProjects/asiainfo/scala/dvb/data \
201604 \
201609 \
1000
