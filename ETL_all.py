import json
from pyspark import SparkConf, SparkContext
import sys
from pyspark.sql import SparkSession, functions, types

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

rating_schema = types.StructType([
    types.StructField('reviewer_id', types.StringType()),
    types.StructField('item_id', types.StringType()),
    types.StructField('rating', types.FloatType()),
    types.StructField('unixReviewTime', types.StringType())
])
review_schema = types.StructType([
    types.StructField('reviewerID', types.StringType()),
    types.StructField('asin', types.StringType()),
    types.StructField('reviewerName', types.StringType()),
    types.StructField('helpful', types.StringType()),
    types.StructField('reviewText', types.StringType()),
    types.StructField('overall', types.FloatType()),
    types.StructField('summary', types.StringType()),
    types.StructField('unixReviewTime', types.StringType()),
    types.StructField('reviewTime', types.StringType())
])
meta_schema = types.StructType([
    types.StructField('asin', types.StringType()),
    types.StructField('title', types.StringType()),
    types.StructField('price', types.FloatType()),
    types.StructField('related', types.StringType()),
    types.StructField('salesRank', types.StringType()),
    types.StructField('brand', types.StringType()),
    types.StructField('categories', types.StringType())
])


def create_rating_data(inputs):
    # Input is review files and meta_data
    # Output is
    # (reviewer_id, item_id, rating, date_time, category)
    rating = spark.read.csv(inputs, schema=rating_schema)
    rating = rating.withColumn('date_time', functions.from_unixtime(rating['unixReviewTime'], 'yyyy-MM-dd')) \
        .drop('unixReviewTime')
    rating = rating.withColumn('category',
                               functions.substring_index(
                                   functions.substring_index(functions.input_file_name(), 'ratings_', -1), '.', 1)
                               )
    rating.show()
    rating.write.csv('all_rating_with_category', mode='overwrite')


def create_review_data(inputs):
    # Input is review files and meta_data
    # Output is
    # (reviewerID, asin, reviewerName, helpful, reviewText, overall, summary, date_time, category
    review = spark.read.json(inputs, schema=review_schema)
    review = review.withColumn('date_time', functions.from_unixtime(review['unixReviewTime'], 'yyyy-MM-dd')) \
        .drop('unixReviewTime').drop('reviewTime')
    review = review.withColumn('category',
                               functions.substring_index(
                                   functions.substring_index(functions.input_file_name(), 'reviews_', -1), '_5.json', 1
                               )
                               )
    review.show()
    review.write.json('all_review_with_category', mode='overwrite')


def create_review_meta(review_file, meta_file):
    # Input is review files and meta_data
    # Output is
    # (asin, reviewerID reviewerName helpful, overall, summary, date_time, category, title, price, brand, categories)
    review = spark.read.json(review_file, schema=review_schema)
    review = review.withColumn('date_time', functions.from_unixtime(review['unixReviewTime'], 'yyyy-MM-dd')) \
        .drop('unixReviewTime').drop('reviewTime').drop('reviewText')
    review = review.withColumn('category',
                               functions.substring_index(
                                   functions.substring_index(functions.input_file_name(), 'reviews_', -1), '_5.json', 1
                               )
                               )
    meta = spark.read.json(meta_file, schema=meta_schema)
    meta = meta.select(
        meta['asin'],
        meta['title'],
        meta['price'],
        meta['brand'],
        functions.regexp_replace(
            (functions.regexp_replace(meta['categories'], "\\[\\[", "")), "\\]\\]", "").alias('categories')
    ).dropna(how='any')
    review_meta = review.join(meta, 'asin')
    review_meta.show()
    review_meta.write.json('review_meta', mode='overwrite')


if __name__ == '__main__':
    spark = SparkSession.builder.appName('ETL_all').getOrCreate()
    assert spark.version >= '2.4'
    spark.sparkContext.setLogLevel('WARN')
    inputs = sys.argv[1]
    if inputs == 'ratings':
        create_rating_data(inputs)
    elif inputs == 'reviews':
        create_review_data(inputs)
    elif inputs == 'review_meta':
        create_review_meta(sys.argv[2], sys.argv[3])
    else:
        print('Invalid argument!')
        print('Usage: \nETL_all.py [ratings/reviews]')
        print('\nETL_all.py [review_meta] [reviews] [meta]')
        exit(0)
