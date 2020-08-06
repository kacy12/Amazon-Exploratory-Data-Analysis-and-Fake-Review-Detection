import json
from pyspark import SparkConf, SparkContext
import sys
import datetime
import pandas as pd
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import lit, avg, col

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+


def single_category(inputs, category, start_date, end_date):
    # Define schema, aligned field name between 2 schemas

    meta_schema = types.StructType([
        types.StructField('asin', types.StringType()),
        types.StructField('title', types.StringType()),
        types.StructField('reviewerID', types.StringType()),
        types.StructField('reviewerName', types.StringType()),
        types.StructField('helpful', types.StringType()),
        types.StructField('date_time', types.DateType()),
        types.StructField('brand', types.StringType()),
        types.StructField('category', types.StringType())
    ])

    # align column names and adding necessary changes to df
    review = spark.read.json(inputs, schema=meta_schema)
    # review.show()
    review = review.select(review['reviewerID'].alias('reviewer_id'),
                           review['asin'].alias('item_id'),
                           review['reviewerName'].alias('reviewer_name'),
                           functions.regexp_extract(review['helpful'], r'\[(\d+),(\d+)\]', 1).alias('helpful'),
                           functions.regexp_extract(review['helpful'], r'\[(\d+),(\d+)\]', 2).alias('not_helpful'),
                           review['date_time'],
                           review['category'],
                           review['title']
                           ).withColumn('year', functions.year(review['date_time'])).withColumn('month',
                                                                                                functions.month(review[
                                                                                                                    'date_time']))

    # select single category and limit time period
    review = review.where(review['category'] == category)
    review = review.where((review['date_time'] >= start_date) & (review['date_time'] <= end_date)).cache()

    review = review.orderBy('title', 'date_time')

    # add a column name count with 1, use for count total review number
    reviewwithNum = review.withColumn('count', lit(1))
    totalnumber = reviewwithNum.select('title', 'date_time', 'year', 'month', 'count')

    itemYear = totalnumber.groupBy('title', 'year').sum('count')
    itemYear = itemYear.orderBy('title', 'year', ascending=False)

    itemMonth = totalnumber.groupBy('title', 'month').sum('count')
    itemMonth = itemMonth.orderBy('title', 'month', ascending=False)

    # select the items with top 10 review number
    top10ItemEachYear = reviewwithNum.select('title', 'date_time', 'count')
    top10ItemEachYear = top10ItemEachYear.groupBy('title').sum('count')
    top10ItemEachYear = top10ItemEachYear.orderBy('title', ascending=False)

    TopNproducts = 10
    top10ItemEachYear.show()
    TopCountProjucts = top10ItemEachYear.orderBy('sum(count)', ascending=False).limit(TopNproducts).select('title')
    TopCountProjucts.show()

    # join the table for top 10 items for TOP10 for year and month
    TopYear = itemYear.join(TopCountProjucts, 'title')
    TopYear.show()

    TopMonth = itemMonth.join(TopCountProjucts, 'title')
    TopMonth.show()
    # +----------+----+-----+---------------+
    # | title    |year|month| sum(count)    |
    # +----------+----+-----+---------------+
    # |B0001VL0K2|2012| 12  | 44            |
    # |B0001VL0K2|2012| 11  | 13            |
    # |B0001VL0K2|2012| 10  | 3             |
    # |B0001VL0K2|2012| 9   | 3             |
    # |B0001VL0K2|2012| 8   | 3             |
    # |B0001VL0K2|2012| 6   | 9             |

    # save the result to output as csv files.
    out_dir_year = category + '_top_review_yearly' + '_' + start_date + '_' + end_date
    TopYear.write.csv(out_dir_year, mode='overwrite')

    out_dir_month = category + '_top_review_monthly' + '_' + start_date + '_' + end_date
    TopMonth.write.csv(out_dir_month, mode='overwrite')


def all_category(inputs, start_date, end_date):
    meta_schema = types.StructType([
        types.StructField('asin', types.StringType()),
        types.StructField('title', types.StringType()),
        types.StructField('reviewerID', types.StringType()),
        types.StructField('reviewerName', types.StringType()),
        types.StructField('helpful', types.StringType()),
        types.StructField('date_time', types.DateType()),
        types.StructField('brand', types.StringType())
    ])

    # align column names and adding necessary changes to df
    review = spark.read.json(inputs, schema=meta_schema)
    # review.show()
    review = review.select(review['reviewerID'].alias('reviewer_id'),
                           review['asin'].alias('item_id'),
                           review['reviewerName'].alias('reviewer_name'),
                           functions.regexp_extract(review['helpful'], r'\[(\d+),(\d+)\]', 1).alias('helpful'),
                           functions.regexp_extract(review['helpful'], r'\[(\d+),(\d+)\]', 2).alias('not_helpful'),
                           review['date_time'],
                           review['title']
                           ).withColumn('year', functions.year(review['date_time'])).withColumn('month',
                                                                                                functions.month(review[
                                                                                                                    'date_time']))

    # select limit time period
    review = review.where((review['date_time'] >= start_date) & (review['date_time'] <= end_date)).cache()

    review = review.orderBy('title', 'date_time')

    # add a column name count with 1, use for count total review number
    reviewwithNum = review.withColumn('count', lit(1))
    totalnumber = reviewwithNum.select('title', 'date_time', 'year', 'month', 'count')

    itemYear = totalnumber.groupBy('title', 'year').sum('count')
    itemYear = itemYear.orderBy('title', 'year', ascending=False)

    itemMonth = totalnumber.groupBy('title', 'month').sum('count')
    itemMonth = itemMonth.orderBy('title', 'month', ascending=False)

    # select the items with top 10 review number
    top10ItemEachYear = reviewwithNum.select('title', 'date_time', 'count')
    top10ItemEachYear = top10ItemEachYear.groupBy('title').sum('count')
    top10ItemEachYear = top10ItemEachYear.orderBy('title', ascending=False)

    TopNproducts = 10
    top10ItemEachYear.show()
    TopCountProjucts = top10ItemEachYear.orderBy('sum(count)', ascending=False).limit(TopNproducts).select('title')
    TopCountProjucts.show()

    # join the table for top 10 items for TOP10 for year and month
    TopYear = itemYear.join(TopCountProjucts, 'title')
    TopYear.show()

    TopMonth = itemMonth.join(TopCountProjucts, 'title')
    TopMonth.show()
    # +----------+----+-----+---------------+
    # | title    |year|month| sum(count)    |
    # +----------+----+-----+---------------+
    # |B0001VL0K2|2012| 12  | 44            |
    # |B0001VL0K2|2012| 11  | 13            |
    # |B0001VL0K2|2012| 10  | 3             |
    # |B0001VL0K2|2012| 9   | 3             |
    # |B0001VL0K2|2012| 8   | 3             |
    # |B0001VL0K2|2012| 6   | 9             |

    # save the result to output as csv files.
    out_dir_year = '_all_review_for_Year' + '_' + start_date + '_' + end_date
    TopYear.write.json(out_dir_year, mode='overwrite')

    out_dir_month = '_all_review_for_Month' + '_' + start_date + '_' + end_date
    TopMonth.write.json(out_dir_month, mode='overwrite')

if __name__ == '__main__':
    spark = SparkSession.builder.appName('TopNumberReview').getOrCreate()
    assert spark.version >= '2.4'
    spark.sparkContext.setLogLevel('WARN')
    mode = sys.argv[1]
    if mode == 'single':
        if len(sys.argv) != 6:
            print("Invalid argument")
            print("Usage:\nTop10ReviewItems.py [single] [input] [start_date] [end_date]")
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
            print("Usage:\nTop10ReviewItems.py [all] [input] [start_date] [end_date]")
            print('date format: YYYY-MM-DD\n')
            exit(0)
        inputs = sys.argv[2]
        start_date = sys.argv[3]
        end_date = sys.argv[4]
        all_category(inputs, start_date, end_date)
    else:
        print("Invalid mode, [single/all]")
        print("Usage:\nTop10ReviewItems.py [single/all] ([category]) [input] [start_date] [end_date]")
        print('date format: YYYY-MM-DD\n')
        exit(0)
