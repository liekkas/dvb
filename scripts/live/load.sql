ALTER TABLE `gags`.`T_LIVE_BROADCAST_SHOWS_T`
ADD INDEX `index1` USING BTREE (`TIME_SCHMER` ASC);

delete from gags.T_LIVE_BROADCAST_SHOWS_T;

-- 加载数据 加载g概况
LOAD DATA LOCAL INFILE 'D:/gags/20161122/data/overview/month/part-00000'
INTO TABLE gags.T_USER_SUMMARY_M character set utf8
FIELDS TERMINATED BY '\t';
LOAD DATA LOCAL INFILE 'D:/gags/20161122/data/overview/week/part-00000'
INTO TABLE gags.T_USER_SUMMARY_W character set utf8
FIELDS TERMINATED BY '\t';
LOAD DATA LOCAL INFILE 'D:/gags/20161122/data/overview/day/part-00000'
INTO TABLE gags.T_USER_SUMMARY_D character set utf8
FIELDS TERMINATED BY '\t';
LOAD DATA LOCAL INFILE 'D:/gags/20161122/data/overview/hour/part-00000'
INTO TABLE gags.T_USER_SUMMARY_T character set utf8
FIELDS TERMINATED BY '\t';

-- 加载数据 月
LOAD DATA LOCAL INFILE 'D:/gags/20161122/data/live/liveByMonth/summary/part-00000'
INTO TABLE gags.T_USER_SUMMARY_M character set utf8
FIELDS TERMINATED BY '\t';
LOAD DATA LOCAL INFILE 'D:/gags/20161122/data/live/liveByMonth/channelType/part-00000'
INTO TABLE gags.T_LIVE_BROADCAST_GROUP_M character set utf8
FIELDS TERMINATED BY '\t';
LOAD DATA LOCAL INFILE 'D:/gags/20161122/data/live/liveByMonth/channel/part-00000'
INTO TABLE gags.T_LIVE_BROADCAST_CHANNEL_M character set utf8
FIELDS TERMINATED BY '\t';

-- 加载数据 周
LOAD DATA LOCAL INFILE 'D:/gags/20161122/data/live/liveByWeek/summary/part-00000'
INTO TABLE gags.T_USER_SUMMARY_W character set utf8
FIELDS TERMINATED BY '\t';
LOAD DATA LOCAL INFILE 'D:/gags/20161122/data/live/liveByWeek/channelType/part-00000'
INTO TABLE gags.T_LIVE_BROADCAST_GROUP_W character set utf8
FIELDS TERMINATED BY '\t';
LOAD DATA LOCAL INFILE 'D:/gags/20161122/data/live/liveByWeek/channel/part-00000'
INTO TABLE gags.T_LIVE_BROADCAST_CHANNEL_W character set utf8
FIELDS TERMINATED BY '\t';

-- 加载数据 天
LOAD DATA LOCAL INFILE 'D:/gags/20161122/data/live/liveByDay/summary/part-00000'
INTO TABLE gags.T_USER_SUMMARY_D character set utf8
FIELDS TERMINATED BY '\t';
LOAD DATA LOCAL INFILE 'D:/gags/20161122/data/live/liveByDay/channelType/part-00000'
INTO TABLE gags.T_LIVE_BROADCAST_GROUP_D character set utf8
FIELDS TERMINATED BY '\t';
LOAD DATA LOCAL INFILE 'D:/gags/20161122/data/live/liveByDay/channel/part-00000'
INTO TABLE gags.T_LIVE_BROADCAST_CHANNEL_D character set utf8
FIELDS TERMINATED BY '\t';

-- 加载数据 时
LOAD DATA LOCAL INFILE 'D:/gags/20161122/data/live/liveByHour/summary/part-00000'
INTO TABLE gags.T_USER_SUMMARY_T character set utf8
FIELDS TERMINATED BY '\t';
LOAD DATA LOCAL INFILE 'D:/gags/20161122/data/live/liveByHour/channelType/part-00000'
INTO TABLE gags.T_LIVE_BROADCAST_GROUP_T character set utf8
FIELDS TERMINATED BY '\t';
LOAD DATA LOCAL INFILE 'D:/gags/20161122/data/live/liveByHour/channel/part-00000'
INTO TABLE gags.T_LIVE_BROADCAST_CHANNEL_T character set utf8
FIELDS TERMINATED BY '\t';

LOAD DATA LOCAL INFILE 'D:/gags/20161122/data/live/liveByHour/show/sortedold/part-00000'
INTO TABLE gags.T_LIVE_BROADCAST_SHOWS_T character set utf8
FIELDS TERMINATED BY '\t';
LOAD DATA LOCAL INFILE 'D:/gags/20161122/data/live/liveByHour/show/sorted/part-00000'
INTO TABLE gags.T_LIVE_BROADCAST_SHOWS_T character set utf8
FIELDS TERMINATED BY '\t';
