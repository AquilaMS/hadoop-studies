from pyspark.sql import SparkSession
import pyspark.sql.functions as f 

if __name__ == '__main__':
        
    spark = SparkSession.builder.appName('cass').config('spark.connection.host', '127.0.0.1').getOrCreate()
    readUsers = spark.read\
        .format('org.apache.spark.sql.cassandra')\
        .options(table = 'imdb', keyspace = 'ks_imdb')\
        .load()

    language_average = readUsers\
        .groupBy('original_language')\
        .agg(f.round(f.avg('vote_average'),2))\
        .withColumnRenamed('round(avg(vote_average), 2)', 'vote_average')
       
    count_language = readUsers\
        .groupBy('original_language')\
        .count()
    language_average.join(count_language, ['original_language'], 'inner').sort(f.desc('count')).show()

    spark.stop()


