/Users/liekkas/env/bd/spark/spark-1.6.2-bin-hadoop2.6/bin/spark-submit \
--class com.citic.guoan.dvb.live.calckpi.SortShowsByUserIndex \
--master local[2] \
/Users/liekkas/IdeaProjects/asiainfo/scala/dvb/target/dvb-0.0.1-SNAPSHOT.jar \
/Users/liekkas/IdeaProjects/asiainfo/scala/dvb/data/live/liveByHour/show/olddata \
/Users/liekkas/IdeaProjects/asiainfo/scala/dvb/data/live/liveByHour/show/sortedold
