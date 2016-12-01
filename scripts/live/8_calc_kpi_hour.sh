/Users/liekkas/env/bd/spark/spark-1.6.2-bin-hadoop2.6/bin/spark-submit \
--class com.citic.guoan.dvb.live.calckpi.CalcKpiByHour \
--master local[4] \
/Users/liekkas/IdeaProjects/asiainfo/scala/dvb/target/dvb-0.0.1-SNAPSHOT.jar \
/Users/liekkas/IdeaProjects/asiainfo/scala/dvb/data/live/test3/splited \
/Users/liekkas/IdeaProjects/asiainfo/scala/dvb/data/live/channel_dict.txt \
/Users/liekkas/IdeaProjects/asiainfo/scala/dvb/data/live/live_prepare_sum.txt \
/Users/liekkas/IdeaProjects/asiainfo/scala/dvb/data/live/test3/liveByHour \
2016-04-01 \
2016-09-30 \
10000
