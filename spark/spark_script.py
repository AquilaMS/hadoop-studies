from pyspark.sql import SparkSession

spark = SparkSession.builder\
    .master('local')\
    .appName('study_spark')\
    .getOrCreate()

df = spark.read.csv('file:////home/aquila/Documents/engdados/hadoop-studies-master/spark/df_netflix.csv', header=True, inferSchema=True, sep='|')

select_col = df.select('title','release_year' ,'imdb_score', 'imdb_votes', 'genres')
select_imdb = select_col\
    .filter(select_col['genres'].contains('animation'))\
    .filter(select_col['imdb_votes'] > 6000)\
    .filter(select_col['release_year'] > 2016)\
    .filter(select_col['imdb_score'] > 7.0)\
    .orderBy(select_col['imdb_votes'], ascending = False)\
    .write.format('csv')\
    .save('file:////home/aquila/Documents/engdados/hadoop-studies-master/spark/out/best_animations_since_2016.csv')


# .save('hdfs:///user/aquila/tests-spark/out/best_animations_since_2016.csv')
# read.csv('hdfs:///user/aquila/tests-spark/df_netflix.csv'