-- 加载数据 月
LOAD DATA LOCAL INFILE '/Users/liekkas/IdeaProjects/asiainfo/scala/dvb/data/demandByMonth/summary/part-00000'
INTO TABLE gags.T_USER_SUMMARY_M character set utf8
FIELDS TERMINATED BY '\t';
LOAD DATA LOCAL INFILE '/Users/liekkas/IdeaProjects/asiainfo/scala/dvb/data/demandByMonth/showType/part-00000'
INTO TABLE gags.T_DEMAND_BROADCAST_M character set utf8
FIELDS TERMINATED BY '\t';
LOAD DATA LOCAL INFILE '/Users/liekkas/IdeaProjects/asiainfo/scala/dvb/data/demandByMonth/show/part-00000'
INTO TABLE gags.T_DEMAND_BROADCAST_SHOWS_M character set utf8
FIELDS TERMINATED BY '\t';

-- 加载数据 周
LOAD DATA LOCAL INFILE '/Users/liekkas/IdeaProjects/asiainfo/scala/dvb/data/demandByWeek/summary/part-00000'
INTO TABLE gags.T_USER_SUMMARY_W character set utf8
FIELDS TERMINATED BY '\t';
LOAD DATA LOCAL INFILE '/Users/liekkas/IdeaProjects/asiainfo/scala/dvb/data/demandByWeek/showType/part-00000'
INTO TABLE gags.T_DEMAND_BROADCAST_W character set utf8
FIELDS TERMINATED BY '\t';
LOAD DATA LOCAL INFILE '/Users/liekkas/IdeaProjects/asiainfo/scala/dvb/data/demandByWeek/show/part-00000'
INTO TABLE gags.T_DEMAND_BROADCAST_SHOWS_W character set utf8
FIELDS TERMINATED BY '\t';

-- 加载数据 天
LOAD DATA LOCAL INFILE '/Users/liekkas/IdeaProjects/asiainfo/scala/dvb/data/demandByDay/summary/part-00000'
INTO TABLE gags.T_USER_SUMMARY_D character set utf8
FIELDS TERMINATED BY '\t';
LOAD DATA LOCAL INFILE '/Users/liekkas/IdeaProjects/asiainfo/scala/dvb/data/demandByDay/showType/part-00000'
INTO TABLE gags.T_DEMAND_BROADCAST_D character set utf8
FIELDS TERMINATED BY '\t';
LOAD DATA LOCAL INFILE '/Users/liekkas/IdeaProjects/asiainfo/scala/dvb/data/demandByDay/show/part-00000'
INTO TABLE gags.T_DEMAND_BROADCAST_SHOWS_D character set utf8
FIELDS TERMINATED BY '\t';

-- 加载数据 时
LOAD DATA LOCAL INFILE '/Users/liekkas/IdeaProjects/asiainfo/scala/dvb/data/demandByHour/summary/part-00000'
INTO TABLE gags.T_USER_SUMMARY_T cHaracter set utf8
FIELDS TERMINATED BY '\t';
LOAD DATA LOCAL INFILE '/Users/liekkas/IdeaProjects/asiainfo/scala/dvb/data/demandByHour/showType/part-00000'
INTO TABLE gags.T_DEMAND_BROADCAST_T character set utf8
FIELDS TERMINATED BY '\t';
LOAD DATA LOCAL INFILE '/Users/liekkas/IdeaProjects/asiainfo/scala/dvb/data/demandByHour/show/part-00000'
INTO TABLE gags.T_DEMAND_BROADCAST_SHOWS_T character set utf8
FIELDS TERMINATED BY '\t';