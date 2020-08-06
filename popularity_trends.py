import json
from pyspark import SparkConf, SparkContext
import sys
from pyspark.sql import SparkSession, types
from pyspark.sql import functions as f

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+


def get_sales_over_time(inputs, start_date, end_date):
    schema = types.StructType([
        types.StructField('asin', types.StringType()),
        types.StructField('overall', types.FloatType()),
        types.StructField('price', types.FloatType()),
        types.StructField('brand', types.StringType()),
        types.StructField('category', types.StringType()),
        types.StructField('date_time', types.DateType())
    ])
    data = spark.read.json(inputs, schema=schema)
    # add necessary extra columns
    data = data.withColumn('year', f.year('date_time'))
    data = data.withColumn('month', f.month('date_time'))
    cate_sale = data.groupBy('category').agg(f.count('category').alias('count'), f.sum('price').alias('sale'))
    cate_sale = cate_sale.orderBy('count', ascending=False)
    cate_sale.show()
    out_dir = 'all_sales_' + start_date + '_' + end_date
    cate_sale.write.json(out_dir, mode='overwrite')

    cate_sale_yearly = data.groupBy('category', 'year').agg(f.count('category').alias('count'),
                                                            f.sum('price').alias('sale')). \
        orderBy(['category', 'year'], ascending=False)
    cate_sale_yearly.show()
    out_dir = 'all_sales_yearly_' + start_date + '_' + end_date
    cate_sale_yearly.write.json(out_dir, mode='overwrite')

    cate_sale_monthly = data.groupBy('category', 'month', 'year').agg(f.count('category').alias('count'),
                                                                      f.sum('price').alias('sale')). \
        orderBy(['category', 'month', 'year'], ascending=False)
    cate_sale_monthly.show()
    out_dir = 'all_sales_monthly_' + start_date + '_' + end_date
    cate_sale_monthly.write.json(out_dir, mode='overwrite')


if __name__ == '__main__':
    spark = SparkSession.builder.appName('popularity_trends').getOrCreate()
    assert spark.version >= '2.4'
    spark.sparkContext.setLogLevel('WARN')
    if len(sys.argv) != 4:
        print("Invalid argument")
        print("Usage:\npopularity_trends.py [input] [start_date] [end_date]")
        print('date format: YYYY-MM-DD\n')
        exit(0)
    inputs = sys.argv[1]
    start_date = sys.argv[2]
    end_date = sys.argv[3]
    get_sales_over_time(inputs, start_date, end_date)
