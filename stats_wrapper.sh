
hive -e "
set hive.execution.engine=mr;


set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager;

show databases;

create table serp.best_rank_urls
(
KEYWORD STRING,
MARKET STRING,
LOCATION STRING,
DEVICE STRING,
CRAWL_DATE DATE,
RANK INT,
URL STRING
)
row format delimited fields terminated by '\t' stored as textfile location '/user/root/hive_output/best_rank_urls';

create table serp.dynamic_keywords
(
KEYWORD STRING,
URL STRING
)
row format delimited fields terminated by '\t' stored as textfile location '/user/root/hive_output/dynamic_keywords';


create table serp.results_similarity
(
KEYWORD STRING,
MARKET STRING,
LOCATION STRING,
COSINE_SIMILARITY FLOAT
)
row format delimited fields terminated by '\t' stored as textfile location '/user/root/hive_output/results_similarity';
"

cd /opt/mapr/spark/spark-2.1.0/bin/

./spark-submit --master yarn --deploy-mode client --num-executors 20 --name "SERP_STATS" --conf "spark.executor.cores=6" --conf "spark.executor.memory=12g" --conf "spark.driver.memory=14g" /user/root/stats.py
