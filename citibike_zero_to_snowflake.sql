/*
Snowflake入門 - ゼロからはじめるSnowflake
https://quickstarts.snowflake.com/guide/getting_started_with_snowflake_ja/index.html?index=..%2F..ja#3
*/

-- 4. データロード準備: テーブル作成
create or replace table trips
(tripduration integer,
starttime timestamp,
stoptime timestamp,
start_station_id integer,
start_station_name string,
start_station_latitude float,
start_station_longitude float,
end_station_id integer,
end_station_name string,
end_station_latitude float,
end_station_longitude float,
bikeid integer,
membership_type string,
usertype string,
birth_year integer,
gender integer);

-- 外部ステージ作成
CREATE STAGE citibike_trips 
	URL = 's3://snowflake-workshop-lab/japan/citibike-trips/';

-- ステージ表示
list @citibike_trips;

--ファイル形式作成
create or replace file format csv type='csv'
  compression = 'auto' field_delimiter = ',' record_delimiter = '\n'
  skip_header = 0 field_optionally_enclosed_by = '\042' trim_space = false
  error_on_column_count_mismatch = false escape = 'none' escape_unenclosed_field = '\134'
  date_format = 'auto' timestamp_format = 'auto' null_if = ('') comment = 'file format for ingesting data for zero to snowflake';

-- ファイル形式表示
show file formats in database citibike_qq;

-- 5. データロード: ロード実行(Small: 40秒)
copy into trips from @citibike_trips file_format=csv PATTERN = '.*csv.*' ;

-- テーブル削除
truncate table trips;

-- テーブル確認
select * from trips limit 10;

-- ウェアハウスサイズ変更(small to large)
alter warehouse compute_wh set warehouse_size='large';

-- ウェアハウス確認
show warehouses;

-- ロード実行(10秒)
copy into trips from @citibike_trips
file_format=CSV;

-- 6. クエリ・結果キャッシュ・クローン操作
-- GUIでウェアハウス作成(analytics_wh)
-- テーブル確認
select * from trips limit 20;

-- 利用状況に関する基本的な時間別統計
select date_trunc('hour', starttime) as "date",
count(*) as "num trips",
avg(tripduration)/60 as "avg duration (mins)",
avg(haversine(start_station_latitude, start_station_longitude, end_station_latitude, end_station_longitude)) as "avg distance (km)"
from trips
group by 1 order by 1;

-- 結果キャッシュ動作
select date_trunc('hour', starttime) as "date",
count(*) as "num trips",
avg(tripduration)/60 as "avg duration (mins)",
avg(haversine(start_station_latitude, start_station_longitude, end_station_latitude, end_station_longitude)) as "avg distance (km)"
from trips
group by 1 order by 1;

-- どの月が忙しいか
select
monthname(starttime) as "month",
count(*) as "num trips"
from trips
group by 1 order by 2 desc;

-- クローン作成
create table trips_dev clone trips;

-- 7.半構造化データ・ビュー・結合: 
create database weather;

-- コンテキスト設定
use role sysadmin;
use warehouse compute_wh;
use database weather;
use schema public;

-- テーブル作成(半構造化データ用)
create table json_weather_data (v variant);

-- 外部ステージ作成(半構造化データ)
create stage nyc_weather
url = 's3://snowflake-workshop-lab/zero-weather-nyc';

-- ステージ表示
list @nyc_weather;

-- 半構造化データロード
copy into json_weather_data
from @nyc_weather 
    file_format = (type = json strip_outer_array = true);

-- テーブル確認
select * from json_weather_data limit 10;

-- ビューの作成
create or replace view json_weather_data_view as
select
    v:obsTime::timestamp as observation_time,
    v:station::string as station_id,
    v:name::string as city_name,
    v:country::string as country,
    v:latitude::float as city_lat,
    v:longitude::float as city_lon,
    v:weatherCondition::string as weather_conditions,
    v:coco::int as weather_conditions_code,
    v:temp::float as temp,
    v:prcp::float as rain,
    v:tsun::float as tsun,
    v:wdir::float as wind_dir,
    v:wspd::float as wind_speed,
    v:dwpt::float as dew_point,
    v:rhum::float as relative_humidity,
    v:pres::float as pressure
from
    json_weather_data
where
    station_id = '72502';

-- 半構造化データのクエリ
select * from json_weather_data_view
where date_trunc('month',observation_time) = '2018-01-01'
limit 20;

-- 特定の天候条件に関連する相応回数(WeatherとTripsを結合、天候が自転車の利用回数にどのような影響を与えるか)
select weather_conditions as conditions
,count(*) as num_trips
from citibike_qq.public.trips
left outer join json_weather_data_view
on date_trunc('hour', observation_time) = date_trunc('hour', starttime)
where conditions is not null
group by 1 order by 2 desc;

-- 8. タイムトラベル: 誤ってもしくは意図的にテーブルを削除
drop table json_weather_data;

-- テーブル確認
select * from json_weather_data limit 10;

-- テーブル復元
undrop table json_weather_data;

-- テーブル確認
select * from json_weather_data limit 10;

-- テーブルのロールバック: コンテキスト設定
use role sysadmin;
use warehouse compute_wh;
use database citibike_qq;
use schema public;

-- 駅名を置き換え
update trips set start_station_name = 'oops';

-- テーブル確認
select
start_station_name as "station",
count(*) as "rides"
from trips
group by 1
order by 2 desc
limit 20;

-- 変数にクエリIDを格納(直近のupdateコマンドを見つける)
set query_id =
(select query_id from table(information_schema.query_history_by_session (result_limit=>5))
where query_text like 'update%' order by start_time desc limit 1);

-- タイムトラベルを使ってテーブル再作成
create or replace table trips as
(select * from trips before (statement => $query_id));

-- テーブル確認
select
start_station_name as "station",
count(*) as "rides"
from trips
group by 1
order by 2 desc
limit 20;


-- 9. ロール、アカウント管理者、アカウント使用状況: コンテキスト設定
use role accountadmin;

-- ロール作成と割り当て
create role junior_dba;
grant role junior_dba to user MMATSUSHITA_SFC;

-- コンテキスト設定
use role junior_dba;

-- コンテキスト設定
use role accountadmin;

-- ウェアハウス使用権限を付与
grant usage on warehouse compute_wh to role junior_dba;

-- コンテキスト設定
use role junior_dba;
use warehouse compute_wh;

-- コンテキスト設定
use role accountadmin;

-- citibikeとweatherテーブルを使用するための権限を付与
grant usage on database citibike_qq to role junior_dba;
grant usage on database weather to role junior_dba;

-- コンテキスト設定
use role junior_dba;

-- アカウント管理者UIの表示: Worksheetからaccountadminに変更

-- 10. 安全なデータ共有とマーケットプレイス(UIから)

-- 環境のリセット
use role accountadmin;
drop share if exists zero_to_snowflake_shared_data;
drop database if exists citibike_qq;
drop database if exists weather;
drop warehouse if exists analytics_wh;
drop role if exists junior_dba;

-- お疲れ様でした！
