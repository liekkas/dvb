/Users/liekkas/env/bd/spark/spark-1.6.2-bin-hadoop2.6/bin/spark-submit \
--class com.citic.guoan.dvb.live.LivePrepare \
--master local[2] \
--jars /Users/liekkas/.m2/repository/redis/clients/jedis/2.8.1/jedis-2.8.1.jar \
/Users/liekkas/IdeaProjects/asiainfo/scala/dvb/target/dvb-0.0.1-SNAPSHOT.jar \
/Users/liekkas/IdeaProjects/asiainfo/scala/dvb/data/live/test/transformed/part-00000 \
/Users/liekkas/IdeaProjects/asiainfo/scala/dvb/data/demand/demand_base_uids.txt \
/Users/liekkas/IdeaProjects/asiainfo/scala/dvb/data/live/live_prepare_sum.txt
