import json
from pyspark import SparkConf, SparkContext
import sys
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import desc
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

def single_category(inputs, category, start_date, end_date):
    combine_schema = types.StructType([
        types.StructField('asin', types.StringType()),
        types.StructField('overall', types.FloatType()),
        types.StructField('price', types.FloatType()),
        types.StructField('brand', types.StringType()),
        types.StructField('category', types.StringType()),
        types.StructField('date_time', types.StringType())
    ])

    review = spark.read.json(inputs, schema=combine_schema)
    review = review.where((review['date_time'] >= start_date) & (review['date_time'] <= end_date))
    review = review.where(review['category'] == category)
    review = review.select(review['asin'].alias('item_id'),
                           review['overall'].alias('rating'),
                           review['price'],
                           review['brand'])

    item_count = review.groupBy('item_id', 'brand').count()
    item_with_brand = item_count.where(review.brand != "")
    item_without_brand = item_count.where(review.brand == "")
    item_price_avg = review.groupBy('item_id').avg('price').withColumnRenamed('avg(price)', 'price')
    item_with_brand = item_with_brand.join(item_price_avg, 'item_id').orderBy('count', ascending=False)
    item_without_brand = item_without_brand.join(item_price_avg, 'item_id').orderBy('count', ascending=False)

    item_with_brand.show()
    item_without_brand.show()
    out_dir_1 = category + '_item_with_brand' + '_' + start_date + '_' + end_date
    out_dir_2 = category + '_item_without_brand' + '_' + start_date + '_' + end_date
    item_with_brand.write.csv(out_dir_1, mode='overwrite')
    item_without_brand.write.csv(out_dir_2, mode='overwrite')

def all_category(inputs, start_date, end_date):
    combine_schema = types.StructType([
        types.StructField('asin', types.StringType()),
        types.StructField('overall', types.FloatType()),
        types.StructField('price', types.FloatType()),
        types.StructField('category', types.StringType()),
        types.StructField('brand', types.StringType()),
        types.StructField('date_time', types.StringType())
    ])

    review = spark.read.json(inputs, schema=combine_schema)
    review = review.where((review['date_time'] >= start_date) & (review['date_time'] <= end_date))
    review = review.select(review['asin'].alias('item_id'),
                           review['overall'].alias('rating'),
                           review['price'],
                           review['category'],
                           review['brand'])

    cate_with_brand = review.where(review.brand != "")
    cate_with_brand_count = cate_with_brand.groupBy('category').count().withColumnRenamed('count','count_with_brand')
    cate_with_brand_price_avg = cate_with_brand.groupBy('category').avg('price').withColumnRenamed('avg(price)', 'price_with_brand')
    cate_count_prize_1 = cate_with_brand_count.join(cate_with_brand_price_avg, 'category')

    cate_without_brand = review.where(review.brand == "")
    cate_without_brand_count = cate_without_brand.groupBy('category').count().withColumnRenamed('count','count_without_brand')
    cate_without_brand_price_avg = cate_without_brand.groupBy('category').avg('price').withColumnRenamed('avg(price)', 'price_without_brand')
    cate_count_prize_2 = cate_without_brand_count.join(cate_without_brand_price_avg, 'category')

    cate_count_prize = cate_count_prize_1.join(cate_count_prize_2, 'category')

    cate_count_prize.show()
    out_dir = 'all_category_brand_vs_no_brand' + '_' + start_date + '_' + end_date
    cate_count_prize.write.json(out_dir, mode='overwrite')


if __name__ == '__main__':
    spark = SparkSession.builder.appName('count_brand').getOrCreate()
    assert spark.version >= '2.4'
    spark.sparkContext.setLogLevel('WARN')

    if not (len(sys.argv) == 5 or len(sys.argv) == 6):
        print("Invalid argument")
        print("Usage:\ncount_brand.py [single/all] [input] [start_date] [end_date]")
        print('date format: YYYY-MM-DD\n')
        exit(0)
    mode = sys.argv[1]
    inputs = sys.argv[2]
    if mode == 'single':
        category = sys.argv[3]
        start_date = sys.argv[4]
        end_date = sys.argv[5]
        single_category(inputs, category, start_date, end_date)
    elif mode == 'all':
        start_date = sys.argv[3]
        end_date = sys.argv[4]
        all_category(inputs, start_date, end_date)
    else:
        print("Invalid mode, [single/all]")
        print("Usage:\ncount_brand.py [single/all] ([category]) [input] [start_date] [end_date]")
        print('date format: YYYY-MM-DD\n')
        exit(0)



