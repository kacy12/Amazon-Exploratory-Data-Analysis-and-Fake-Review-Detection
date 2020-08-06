import json
from pyspark import SparkConf, SparkContext
import sys
from pyspark.sql import SparkSession, types
from pyspark.sql.functions import desc
from pyspark.sql import functions as f

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+


def single_category(inputs, category, start_date, end_date):
    rating_schema = types.StructType([
        types.StructField('title', types.StringType()),
        types.StructField('overall', types.FloatType()),
        types.StructField('date_time', types.DateType()),
        types.StructField('category', types.StringType())
    ])
    rating = spark.read.json(inputs, schema=rating_schema)
    rating = rating.where(rating['category'] == category)
    rating = rating.where((rating['date_time'] >= start_date) & (rating['date_time'] <= end_date))
    rating = rating.withColumn('year', f.year('date_time'))
    rating = rating.withColumn('month', f.month('date_time'))
    rate_avg = rating.groupBy('title').agg(f.count('title').alias('count'), f.avg('overall').alias('avg_rating'))
    rate_avg = rate_avg.orderBy('count', ascending=False)
    rate_avg.show()
    out_dir = category + '_avg_rating' + '_' + start_date + '_' + end_date
    rate_avg.write.json(out_dir, mode='overwrite')

    rate_avg_yearly = rating.groupBy('title', 'year').agg(f.count('title').alias('count'),
                                                          f.avg('overall').alias('avg_rating')). \
        orderBy(['title', 'year'], ascending=False)
    rate_avg_yearly.show()
    out_dir = category + '_avg_rating_yearly_' + start_date + '_' + end_date
    rate_avg_yearly.write.json(out_dir, mode='overwrite')

    rate_avg_monthly = rating.groupBy('title', 'month').agg(f.count('title').alias('count'),
                                                            f.avg('overall').alias('avg_rating')). \
        orderBy(['title', 'month'], ascending=False)
    rate_avg_monthly.show()
    out_dir = category + '_avg_rating_monthly_' + start_date + '_' + end_date
    rate_avg_monthly.write.json(out_dir, mode='overwrite')


def all_category(inputs, start_date, end_date):
    rating_schema = types.StructType([
        types.StructField('title', types.StringType()),
        types.StructField('overall', types.FloatType()),
        types.StructField('date_time', types.DateType()),
        types.StructField('category', types.StringType())
    ])
    rating = spark.read.json(inputs, schema=rating_schema)
    rating = rating.where((rating['date_time'] >= start_date) & (rating['date_time'] <= end_date))
    rating = rating.withColumn('year', f.year('date_time'))
    rating = rating.withColumn('month', f.month('date_time'))
    rate_avg = rating.groupBy('category').agg(f.count('category').alias('count'), f.avg('overall').alias('avg_rating'))
    rate_avg = rate_avg.orderBy('count', ascending=False)
    rate_avg.show()
    out_dir = 'all_avg_rating' + '_' + start_date + '_' + end_date
    rate_avg.write.json(out_dir, mode='overwrite')

    rate_avg_yearly = rating.groupBy('category', 'year').agg(f.count('category').alias('count'),
                                                             f.avg('overall').alias('avg_rating')). \
        orderBy(['category', 'year'], ascending=False)
    rate_avg_yearly.show()
    out_dir = 'all_avg_rating_yearly_' + start_date + '_' + end_date
    rate_avg_yearly.write.json(out_dir, mode='overwrite')

    rate_avg_monthly = rating.groupBy('category', 'month', 'year').agg(f.count('category').alias('count'),
                                                               f.avg('overall').alias('avg_rating')). \
        orderBy(['category', 'month', 'year'], ascending=False)
    rate_avg_monthly.show()
    out_dir = 'all_avg_rating_monthly_' + start_date + '_' + end_date
    rate_avg_monthly.write.json(out_dir, mode='overwrite')


if __name__ == '__main__':
    spark = SparkSession.builder.appName('rating_stats').getOrCreate()
    assert spark.version >= '2.4'
    spark.sparkContext.setLogLevel('WARN')

    mode = sys.argv[1]
    if mode == 'single':
        if len(sys.argv) != 6:
            print("Invalid argument")
            print("Usage:\nrating_stats.py [single] [input] [category] [start_date] [end_date]")
            print('date format: YYYY-MM-DD\n')
            exit(0)
        inputs = sys.argv[2]
        category = sys.argv[3]
        start_date = sys.argv[4]
        end_date = sys.argv[5]
        single_category(inputs, category, start_date, end_date)
    elif mode == 'all':
        if len(sys.argv) != 5:
            print("Invalid argument")
            print("Usage:\nrating_stats.py [all] [input] [start_date] [end_date]")
            print('date format: YYYY-MM-DD\n')
            exit(0)
        inputs = sys.argv[2]
        start_date = sys.argv[3]
        end_date = sys.argv[4]
        all_category(inputs, start_date, end_date)
    else:
        print("Invalid mode, should be [single/all]")
        print("Usage:\nrating_stats.py [single/all] [input] [category](optional) [start_date] [end_date]")
        print('date format: YYYY-MM-DD\n')
        exit(0)
