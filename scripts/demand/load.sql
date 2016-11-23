delete from gags.T_USER_SUMMARY_M where date_time > 201601 and USER_TYPE = 2;
delete from gags.T_USER_SUMMARY_W where date_time > 201601 and USER_TYPE = 2;
delete from gags.T_USER_SUMMARY_D where date_time > '2016-01-01' and USER_TYPE = 2;
delete from gags.T_USER_SUMMARY_T where date_time > '2016-01-01' and USER_TYPE = 2;

delete from gags.T_DEMAND_BROADCAST_M where date_time > 201601;
delete from gags.T_DEMAND_BROADCAST_W where date_time > 201601;
delete from gags.T_DEMAND_BROADCAST_D where date_time > '2016-01-01';
delete from gags.T_DEMAND_BROADCAST_T where date_time > '2016-01-01';

delete from gags.T_DEMAND_BROADCAST_SHOWS_M where date_time > 201601;
delete from gags.T_DEMAND_BROADCAST_SHOWS_W where date_time > 201601;
delete from gags.T_DEMAND_BROADCAST_SHOWS_D where date_time > '2016-01-01';
delete from gags.T_DEMAND_BROADCAST_SHOWS_T where date_time > '2016-01-01';

-- 加载数据 月
LOAD DATA LOCAL INFILE 'D:/gags/20161102/demandByMonth/summary/part-00000'
INTO TABLE gags.T_USER_SUMMARY_M character set utf8
FIELDS TERMINATED BY '\t';
LOAD DATA LOCAL INFILE 'D:/gags/20161102/demandByMonth/showType/part-00000'
INTO TABLE gags.T_DEMAND_BROADCAST_M character set utf8
FIELDS TERMINATED BY '\t';
LOAD DATA LOCAL INFILE 'D:/gags/20161102/demandByMonth/show/part-00000'
INTO TABLE gags.T_DEMAND_BROADCAST_SHOWS_M character set utf8
FIELDS TERMINATED BY '\t';

-- 加载数据 周
LOAD DATA LOCAL INFILE 'D:/gags/20161102/demandByWeek/summary/part-00000'
INTO TABLE gags.T_USER_SUMMARY_W character set utf8
FIELDS TERMINATED BY '\t';
LOAD DATA LOCAL INFILE 'D:/gags/20161102/demandByWeek/showType/part-00000'
INTO TABLE gags.T_DEMAND_BROADCAST_W character set utf8
FIELDS TERMINATED BY '\t';
LOAD DATA LOCAL INFILE 'D:/gags/20161102/demandByWeek/show/part-00000'
INTO TABLE gags.T_DEMAND_BROADCAST_SHOWS_W character set utf8
FIELDS TERMINATED BY '\t';

-- 加载数据 天
LOAD DATA LOCAL INFILE 'D:/gags/20161102/demandByDay/summary/part-00000'
INTO TABLE gags.T_USER_SUMMARY_D character set utf8
FIELDS TERMINATED BY '\t';
LOAD DATA LOCAL INFILE 'D:/gags/20161102/demandByDay/showType/part-00000'
INTO TABLE gags.T_DEMAND_BROADCAST_D character set utf8
FIELDS TERMINATED BY '\t';
LOAD DATA LOCAL INFILE 'D:/gags/20161102/demandByDay/show/part-00000'
INTO TABLE gags.T_DEMAND_BROADCAST_SHOWS_D character set utf8
FIELDS TERMINATED BY '\t';

-- 加载数据 时
LOAD DATA LOCAL INFILE 'D:/gags/20161102/demandByHour/summary/part-00000'
INTO TABLE gags.T_USER_SUMMARY_T cHaracter set utf8
FIELDS TERMINATED BY '\t';
LOAD DATA LOCAL INFILE 'D:/gags/20161102/demandByHour/showType/part-00000'
INTO TABLE gags.T_DEMAND_BROADCAST_T character set utf8
FIELDS TERMINATED BY '\t';
LOAD DATA LOCAL INFILE 'D:/gags/20161102/demandByHour/show/part-00000'
INTO TABLE gags.T_DEMAND_BROADCAST_SHOWS_T character set utf8
FIELDS TERMINATED BY '\t';