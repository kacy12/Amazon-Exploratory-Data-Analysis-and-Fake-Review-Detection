import json
from pyspark import SparkConf, SparkContext
import sys
from pyspark.sql import SparkSession, types
from pyspark.sql import functions as f

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+


def single_category(inputs, category, start_date, end_date):
    schema = types.StructType([
        types.StructField('asin', types.StringType()),
        types.StructField('overall', types.FloatType()),
        types.StructField('price', types.FloatType()),
        types.StructField('brand', types.StringType()),
        types.StructField('category', types.StringType()),
        types.StructField('date_time', types.DateType())
    ])
    data = spark.read.json(inputs, schema=schema)
    data = data.where(data['category'] == category)
    data = data.where((data['date_time'] >= start_date) & (data['date_time'] <= end_date))
    data = data.withColumn('year', f.year('date_time'))
    brand_sale = data.groupBy('brand', 'year').agg(f.count('brand').alias('count'), f.sum('price').alias('sale'))
    brand_sale_avg = brand_sale.withColumn('avg_price', brand_sale['sale'] / brand_sale['count']). \
        orderBy(['year', 'count'], ascending=False)
    brand_sale_avg.show()
    # (brand, [year, count], sale, avg_price)
    out_dir = category + '_brand_sale_' + start_date + '_' + end_date
    brand_sale_avg.write.json(out_dir, mode='overwrite')

    brand_sale_year = data.groupBy('brand', 'year').agg(f.count('brand').alias('count'), f.sum('price').alias('sale')). \
        orderBy(['brand', 'year'], ascending=False)
    brand_sale_year.show()
    # ([brand, year], count, sale)
    out_dir = category + '_brandYear_sale_' + start_date + '_' + end_date
    brand_sale_year.write.json(out_dir, mode='overwrite')

    data = data.withColumn('month', f.month('date_time'))
    brand_sale_month = data.groupBy('brand', 'month', 'year').agg(f.count('brand').alias('count'),
                                                          f.sum('price').alias('sale')). \
        orderBy(['brand', 'month', 'year'], ascending=False)
    brand_sale_month.show()
    # ([brand, month, year], count, sale)
    out_dir = category + '_brandMonth_sale_' + start_date + '_' + end_date
    brand_sale_month.write.json(out_dir, mode='overwrite')


def all_category(inputs, start_date, end_date):
    schema = types.StructType([
        types.StructField('asin', types.StringType()),
        types.StructField('overall', types.FloatType()),
        types.StructField('price', types.FloatType()),
        types.StructField('brand', types.StringType()),
        types.StructField('category', types.StringType()),
        types.StructField('date_time', types.DateType())
    ])
    data = spark.read.json(inputs, schema=schema)
    data = data.where((data['date_time'] >= start_date) & (data['date_time'] <= end_date))
    data = data.withColumn('year', f.year('date_time'))
    brand_sale = data.groupBy('brand', 'year').agg(f.count('brand').alias('count'), f.sum('price').alias('sale'))
    brand_sale_avg = brand_sale.withColumn('avg_price', brand_sale['sale'] / brand_sale['count']). \
        orderBy('count', ascending=False)
    brand_sale_avg.show()
    # ([brand, year], count, sale, avg_price)
    out_dir = 'all_brand_sale_' + start_date + '_' + end_date
    brand_sale_avg.write.json(out_dir, mode='overwrite')


if __name__ == '__main__':
    spark = SparkSession.builder.appName('brand_effect_analysis').getOrCreate()
    assert spark.version >= '2.4'
    spark.sparkContext.setLogLevel('WARN')
    mode = sys.argv[1]
    if mode == 'single':
        if len(sys.argv) != 6:
            print("Invalid argument")
            print("Usage:\nbrand_effect_analysis.py single [input] [category] [start_date] [end_date]")
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
            print("Usage:\nbrand_effect_analysis.py all [input] [start_date] [end_date]")
            print('date format: YYYY-MM-DD\n')
            exit(0)
        inputs = sys.argv[2]
        start_date = sys.argv[3]
        end_date = sys.argv[4]
        all_category(inputs, start_date, end_date)
    else:
        print("Invalid mode")
        print("Usage:\nbrand_effect_analysis.py [single/all] [input] [category](optional) [start_date] [end_date]")
        print('date format: YYYY-MM-DD\n')
        exit(0)
