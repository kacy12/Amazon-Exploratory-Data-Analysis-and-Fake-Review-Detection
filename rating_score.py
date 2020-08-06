import json
from pyspark import SparkConf, SparkContext
import sys
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import desc, lit
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

def single_category(inputs, output):
    # Input is rating file with one category
    # Output is (item_id, count, avg_rating)

    # Define schema
    rating_schema = types.StructType([
        types.StructField('asin', types.StringType()),
        types.StructField('overall', types.FloatType()),
        types.StructField('price', types.FloatType())
    ])

    rating = spark.read.json(inputs, schema=rating_schema)
    rating = rating.select(rating['asin'].alias('item_id'), rating['overall'].alias('rating'))
    rating_great = rating.where((rating.rating == 4) | (rating.rating == 5)).withColumn('rating_score', lit(1))
    rating_normal = rating.where((rating.rating == 2) | (rating.rating == 3)).withColumn('rating_score', lit(0))
    rating_bad = rating.where((rating.rating == 0) | (rating.rating == 1)).withColumn('rating_score', lit(-1))
    rating = rating_great.union(rating_normal)
    rating = rating.union(rating_bad)
    item_score_avg = rating.groupBy('asin').sum('rating_score')\
        .withColumnRenamed('sum(rating_score)', 'rating_score').orderBy('rating_score', ascending=False)

    item_score_avg.show()
    # item_avg_count.write.csv(output)


def all_category(inputs, output):
    # Input is rating file with all categories
    # Output is (category, count, avg_rating)

    # Define schema
    rating_schema = types.StructType([
        types.StructField('asin', types.StringType()),
        types.StructField('overall', types.FloatType()),
        types.StructField('price', types.FloatType()),
        types.StructField('category', types.StringType())
    ])
    rating = spark.read.json(inputs, schema=rating_schema)
    rating = rating.select(rating['asin'].alias('item_id'), rating['overall'].alias('rating'), rating['category'])
    rating_great = rating.where((rating.rating == 4) | (rating.rating == 5)).withColumn('rating_score', lit(1))
    rating_normal = rating.where((rating.rating == 2) | (rating.rating == 3)).withColumn('rating_score', lit(0))
    rating_bad = rating.where((rating.rating == 0) | (rating.rating == 1)).withColumn('rating_score', lit(-1))
    rating = rating_great.union(rating_normal).union(rating_bad)

    category_score_avg = rating.groupBy('category').sum('rating_score') \
        .withColumnRenamed('sum(rating_score)', 'rating_score').orderBy('rating_score', ascending=False)
    category_score_avg.show()

    # category_avg_count.write.csv(output)


if __name__ == '__main__':
    spark = SparkSession.builder.appName('ETL').getOrCreate()
    assert spark.version >= '2.4'
    spark.sparkContext.setLogLevel('WARN')
    mode = sys.argv[1]
    inputs = sys.argv[2]
    output = sys.argv[3]
    if len(sys.argv) != 4:
        print("Invalid argument")
        print("Usage:\nrating_score.py [single/all] [input] [output]")
        exit(0)
    if mode == 'single':
        single_category(inputs, output)
    elif mode == 'all':
        all_category(inputs, output)
    else:
        print("Invalid argument")
        print("Usage:\nrating_score.py [single/all] [input] [output]")
        exit(0)
