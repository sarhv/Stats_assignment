
from pyspark import HiveContext	as sqlContext
from pyspark import SparkConf,SparkContext

sc = SparkContext()
sqlContext=HiveContext(sc)

from pyspark.sql.types import FloatType
from math import sqrt
from pyspark.sql.types import StringType
from pyspark.sql.functions import col, collect_list, concat_ws, udf

#user defined function to compute cosine similarity


def get_cosine(data):
	list1=[]
	list2=[]
	for records in data:
		line = records.split(',')
		if line[0].encode('utf-8') =='desktop':
			print line[0].encode('utf-8')
			list1.append(line[1].encode('utf-8'))
			list1.extend(line[2].encode('utf-8').split('/'))
		else:
			print line[0].encode('utf-8')
			list2.append(line[1].encode('utf-8'))
			list2.extend(line[2].encode('utf-8').split('/'))
	if len(list1)==0 or len(list2)==0:
		return 0
	words1 = tuple([word for word in list1 if word != ''])
	words2 = tuple([word for word in list2 if word != ''])
	wordcount1={}
	for word in words1:
		if word not in wordcount1:
			wordcount1[word] = 1
		else:
			wordcount1[word] += 1
	wordcount2={}
	for word in words2:
		if word not in wordcount2:
			wordcount2[word] = 1
		else:
			wordcount2[word] += 1
	vec1=wordcount1
	vec2=wordcount2
	print vec1
	print vec2
	intersection1 = set(vec1.keys()) & set(vec2.keys())
	numerator1=0
	for x in intersection1:
		numerator1+=vec1[x] * vec2[x]
		#print "loop2",numerator1
	suma1 = sum([vec1[x]**2 for x in vec1.keys() if x <> 'null'])
	suma2 = sum([vec2[x]**2 for x in vec2.keys() if x <> 'null'])
	denominator1 = sqrt(suma1) * sqrt(suma2)
	if not denominator1:
		result1=0.0
	else:
		result1 = float(numerator1) / denominator1
	return result1

##Which URL has the most ranks below 10 across all keywords over the period?	
	
doc=sc.textFile("maprfs:///user/root/serp.csv",use_unicode=False).map(lambda x: x.replace(",","\t"))
header= doc.first()
data = doc.filter(lambda row : row <> header).map(lambda x: x.split("\t"))
clean_data = data.filter(lambda line: len(line) == 7)
seo_data = clean_data.map(lambda x : (x[0],x[1],x[2],x[3],x[4],x[5],x[6]))
good_urls = seo_data.filter(lambda x : float(x[5]) < 10 )
good_urls_df = sqlContext.createDataFrame(good_urls, schema=['keyword','market','location','device','date','rank','url'])
good_urls_df.registerTempTable('good_urls_df')
best_urls_df= good_urls_df[['url']].groupby(['url']).count().sort(col("count").desc())
best_urls_df.registerTempTable('best_urls_df')
sqlContext.sql('truncate table serp.best_rank_urls')
sqlContext.sql('insert into serp.best_rank_urls SELECT * FROM best_urls_df limit 1')


#Provide the set of keywords (keyword information) where the rank 1 URL changes the most over the period. 
#A change, for the purpose of this question, is when a given keyword's rank 1 URL is different from the previous day's URL.

keyval = seo_data.map(lambda x : (x[0],x[1],x[4],x[5],x[6]))
keyval_rank = keyval.filter(lambda x : float(x[3])==1)
rank1_df = sqlContext.createDataFrame(keyval_rank, schema=['keyword','market','date','rank','url'])
rank1_df.registerTempTable('rank1_df')
df1 = sqlContext.sql('SELECT distinct keyword, url from rank1_df')
dynamic = df1[['keyword','url']].groupby(['keyword']).count().sort(col("count").desc())
dynamic.registerTempTable('dynamic')
sqlContext.sql('truncate table serp.dynamic_keywords')
sqlContext.sql('insert into serp.dynamic_keywords SELECT * FROM dynamic')

#Compute similarity of the results returned for the same keyword, market, and location for across devices
sim_input = sqlContext.createDataFrame(seo_data, schema=['keyword','market', 'location', 'device', 'date','rank','url'])
sim_input.registerTempTable('sim_input')
sqlContext.udf.register("get_cosine", get_cosine,FloatType())
diff_devices_df = sqlContext.sql('select a.keyword, a.market, a.location, count( distinct a.device) as count_device from sim_input a group by a.keyword, a.market, a.location  having count_device > 1')
diff_devices_df.registerTempTable('diff_devices_df')
selfjoin = sqlContext.sql('select a.keyword, a.market, a.location,a.device,a.date, a.rank, a.url from sim_input a, diff_devices_df b where  a.market=b.market and a.location= b.location and a.keyword = b.keyword')
selfjoin.registerTempTable('selfjoin')
group_df2= selfjoin.withColumn('data', concat_ws(',', col('device'),col('rank'), col('url'))).groupBy(['keyword','market','location']).agg(collect_list('data').alias('data'))
group_df2.registerTempTable('group_df2')
compute_sim = sqlContext.sql('select group_df2.keyword,group_df2.market, group_df2.location,get_cosine(group_df2.data) from group_df2')
sqlContext.sql('truncate table serp.compute_sim')
sqlContext.sql('insert into serp.results_similarity SELECT * FROM compute_sim')



