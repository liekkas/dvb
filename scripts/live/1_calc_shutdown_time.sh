#!/usr/bin/env bash
/Users/liekkas/env/bd/spark/spark-1.6.2-bin-hadoop2.6/bin/spark-submit \
--class com.citic.guoan.dvb.live.CalcShutdownTime \
--master local[4] \
/Users/liekkas/IdeaProjects/asiainfo/scala/dvb/target/dvb-0.0.1-SNAPSHOT.jar \
/Users/liekkas/IdeaProjects/asiainfo/scala/dvb/data/live/test3/origin \
/Users/liekkas/IdeaProjects/asiainfo/scala/dvb/data/live/test3/shutdown \
50