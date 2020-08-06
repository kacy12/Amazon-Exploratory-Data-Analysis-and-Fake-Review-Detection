import json
from pyspark import SparkConf, SparkContext
import sys
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import desc
from pyspark.sql.window import Window

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

# Input is
# (asin, reviewerID reviewerName helpful, overall, summary, date_time, category, title, price, brand, categories)
data_schema = types.StructType([
    types.StructField('asin', types.StringType()),
    types.StructField('reviewerID', types.StringType()),
    types.StructField('reviewerName', types.StringType()),
    types.StructField('helpful', types.StringType()),
    types.StructField('overall', types.FloatType()),
    types.StructField('summary', types.StringType()),
    types.StructField('date_time', types.DateType()),
    types.StructField('category', types.StringType()),
    types.StructField('title', types.StringType()),
    types.StructField('price', types.FloatType()),
    types.StructField('brand', types.StringType()),
    types.StructField('categories', types.StringType())
])


def get_user(inputs, start_date, end_date):
    data = spark.read.json(inputs, schema=data_schema)
    # select data falls into target time range
    data = data.where((data['date_time'] >= start_date) & (data['date_time'] <= end_date))
    # data.show()
    user_count = data.select('reviewerID', 'overall').groupBy('reviewerID').\
        agg(functions.count('overall').alias('count'))
    print("\nGetting stats on high rating reviews\n")
    user_5count = data.select('reviewerID', 'overall').where(data['overall'] == '5.0').groupBy('reviewerID').\
        agg(functions.count('overall').alias('5_count'))
    print("\nGetting reviewer with review counts above average\n")
    user_ct_5ct = user_count.join(user_5count, 'reviewerID')
    idct_sum = user_ct_5ct.agg(functions.count('reviewerID'), functions.sum('count'))
    avg_ct = idct_sum.collect()[0][1] / idct_sum.collect()[0][0]
    user_ct_5ct = user_ct_5ct.where(user_ct_5ct['count'] >= avg_ct)
    print("\nGetting reviewer with review counts far above average")
    print("and has extreme high percentage on rating 5.0\n")
    user_ct_5ct = user_ct_5ct.select('reviewerID', 'count', '5_count')\
        .withColumn('percentage', 100 * functions.col('5_count') / functions.col('count'))
    user_ct_5ct = user_ct_5ct.where(user_ct_5ct['count'] > 50).sort(desc('percentage'))
    user_list = user_ct_5ct.where(user_ct_5ct['percentage'] > 70).select('reviewerID')
    # user_ct_5ct.show()
    # user_list.show()
    print("\nGetting reviews has high frequency of similar summary text\n")
    user_item = data.join(user_list, 'reviewerID')
    user_item = user_item.withColumn('s_summary', functions.substring(user_item['summary'], 0, 10))
    gp_summary = user_item.groupBy('s_summary').count()
    gp_summary = gp_summary.orderBy('count', ascending=False).collect()[0][0]
    result = user_item.select('asin', 'reviewerID', 'reviewerName', 'summary', 's_summary')\
        .where(user_item['s_summary'] == gp_summary)
    # result.show()
    print("\nGetting reviewerID who has constant reviews on same category product")
    print("and has most review counts\n")
    gp_result = result.groupBy('s_summary', 'reviewerID').count()
    gp_result = gp_result.orderBy('count', ascending=False)
    # gp_result.show()
    winner_id = gp_result.collect()[0][1]
    result = data.where(data['reviewerID'] == winner_id).orderBy('date_time')
    # result.show()
    return result


def further_analysis(data, start_date, end_date):
    reviewerID = data.select('reviewerID').collect()[0][0]
    cate_data = data.groupBy('category').agg(functions.count('category').alias('count'))
    single_category_ct = cate_data.select('category', 'count').orderBy('count', ascending=False)
    all_ct = data.count()
    # single_category_ct.show()
    cate = single_category_ct.collect()[0][0]
    single_category_ct = single_category_ct.collect()[0][1]
    print("\nReviewer {0} has {1:2.4f}% reviews on {2} products\n".format(reviewerID,
                                                                          (100 * single_category_ct / all_ct), cate))
    print("Checking review details of user {}".format(reviewerID))
    print("Checking review frequency based on time line\n")
    my_window = Window.partitionBy('reviewerID').orderBy('date_time')
    user_date = data.select('reviewerID', 'date_time').withColumn('prev_date',
                                                                  functions.lag(data['date_time']).over(my_window))
    user_date = user_date.withColumn('date_diff',
                                     functions.when(
                                         functions.isnull(functions.datediff('date_time', 'prev_date')), 0).
                                     otherwise(functions.datediff('date_time', 'prev_date')))
    avg_date_diff = user_date.groupBy().avg('date_diff').collect()[0][0]
    max_date_diff = user_date.agg(functions.max('date_diff')).collect()[0][0]
    # user_date.show()
    user_date = user_date.withColumn('year', functions.year('date_time'))
    user_date = user_date.withColumn('month', functions.month('date_time'))
    out_dir = 'fake_interval_' + start_date + '_' + end_date
    user_date.write.json(out_dir, mode='overwrite')
    print("\nMax_date_diff is {0} days and average_date_diff is {1:3.2f} days\n".format(max_date_diff, avg_date_diff))
    if avg_date_diff < 20:
        print("\nAfter verification, the possibility of fake review is very high. Generating graph\n")
        out_data = data.drop('helpful', 'categories')
        out_dir = 'fake_detail_' + start_date + '_' + end_date
        out_data.write.json(out_dir, mode='overwrite')
    else:
        print("\nAfter verification, the possibility of fake reviews has been ruled out\n")


if __name__ == '__main__':
    spark = SparkSession.builder.appName('fake_review_detection').getOrCreate()
    assert spark.version >= '2.4'
    spark.sparkContext.setLogLevel('WARN')
    # Program takes agrv[1] = review_meta, argv[2] = start_date, argv[3] = end_date
    if len(sys.argv) != 4:
        print('Invalid argument!')
        print('Usage: fake_review_detection.py review_meta [start_date] [end_date]')
        print('date format: YYYY-MM-DD\n')
        exit(0)
    inputs = sys.argv[1]
    start_date = sys.argv[2]
    end_date = sys.argv[3]
    df = get_user(inputs, start_date, end_date)
    further_analysis(df, start_date, end_date)
