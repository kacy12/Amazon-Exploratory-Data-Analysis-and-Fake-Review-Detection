import json
from pyspark import SparkConf, SparkContext
import sys
import datetime
import pandas as pd
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import lit, avg, col

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

def main(inputs,ID):
    meta_schema = types.StructType([
        types.StructField('asin', types.StringType()),
        # base on this reviewerID
        types.StructField('reviewerID', types.StringType()),
        types.StructField('reviewerName', types.StringType()),
        types.StructField('helpful', types.StringType()),
        types.StructField('overall', types.FloatType()),
        types.StructField('summary', types.StringType()),
        types.StructField('date_time', types.DateType()),
        types.StructField('price', types.FloatType()),
        types.StructField('title', types.StringType()),
        types.StructField('brand', types.StringType()),
        types.StructField('category', types.StringType())
    ])

    # align column names and adding necessary changes to df
    review = spark.read.json(inputs, schema=meta_schema)
    #review.show()
    review = review.select(review['reviewerID'].alias('reviewer_id'),
                           review['price'],
                           review['category']
                           )

    PersonOrder = review.where(review['reviewer_id'] == ID)
    PersonOrder.show()
    PersonOrder = PersonOrder.withColumn('count', lit(1))
    PersonOrder = PersonOrder.groupby('reviewer_id', 'category').sum('count', 'price')
    PersonOrder.show()

    sumOfCount = PersonOrder.groupby('reviewer_id').sum().collect()[0][1]
    sumOfPrice = PersonOrder.groupby('reviewer_id').sum().collect()[0][2]
    print(sumOfCount)
    print(sumOfPrice)

    PersonOrder = PersonOrder.withColumn('Percentage of Order Number', PersonOrder['sum(count)'] / sumOfCount*100)
    PersonOrder = PersonOrder.withColumn('Percentage of Money Spend', PersonOrder['sum(price)'] / sumOfPrice*100)
    PersonOrder = PersonOrder.withColumn('Percentage of category', PersonOrder['sum(count)'] / sumOfCount * 100)

    PersonOrder.show()
    # +--------------+-------------+----------+------------------+--------------------------+-------------------------+
    # | reviewer_id  | category    |sum(count)| sum(price)       | Percentage ofOrder Number| Percentageof  MoneySpend|
    # +--------------+-------------+----------+------------------+--------------------------+-------------------------+
    # |A16CZRQL23NOIW|Movies_and_TV| 926      |12136.959965575486| 0.9935622317596566       | 0.9944073110523517      |
    # |A16CZRQL23NOIW|Digital_Music| 6        |68.25999879837036 | 0.006437768240343348     | 0.005592688947648...    |
    out_Most_Item = '_all_review_Percentage_By_ID'+ '_' + ID
    PersonOrder.write.json(out_Most_Item, mode='overwrite')



if __name__ == '__main__':
    spark = SparkSession.builder.appName('topPrecentage').getOrCreate()
    assert spark.version >= '2.4'
    spark.sparkContext.setLogLevel('WARN')
    inputs = sys.argv[1]
    ID = sys.argv[2]


    main(inputs, ID)

