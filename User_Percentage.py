import json
from pyspark import SparkConf, SparkContext
import sys
import datetime
import pandas as pd
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import lit, avg, col

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

def main(inputs):
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
                           review['reviewerName'].alias('reviewer_name'),
                           review['price'],
                           review['category']
                           )
                          

    #Find reviewer that purchase most items
    selectTop_num = review.withColumn('count', lit(1))
    #review.show()

    Top_Num = selectTop_num.groupby('reviewer_id').sum('count')
    Top_Num = Top_Num.orderBy('sum(count)',ascending=False)
    #Top_Num.show()
    SelectTopNum = 1
    Top_Num = Top_Num.limit(SelectTopNum).select('reviewer_id')
    #Top_Num.show()
    PersonOrder = review.join(Top_Num,'reviewer_id')
    #PersonOrder.show()
    PersonOrder = PersonOrder.select('reviewer_id', 'category','price')
    #PersonOrder.show()
    PersonOrder = PersonOrder.withColumn('count', lit(1))
    #PersonOrder.show()
    PersonOrder = PersonOrder.groupby('reviewer_id', 'category').sum('count', 'price')
    #PersonOrder.show()

    sumOfCount = PersonOrder.groupby('reviewer_id').sum().collect()[0][1]
    sumOfPrice = PersonOrder.groupby('reviewer_id').sum().collect()[0][2]
    print(sumOfPrice)
    PersonOrder = PersonOrder.withColumn('Percentage of Order Number', PersonOrder['sum(count)'] / sumOfCount*100)
    PersonOrder = PersonOrder.withColumn('Percentage of Money Spend', PersonOrder['sum(price)'] / sumOfPrice*100)

    PersonOrder.show()
    # +--------------+-------------+----------+------------------+--------------------------+-------------------------+
    # | reviewer_id  | category    |sum(count)| sum(price)       | Percentage ofOrder Number| Percentageof  MoneySpend|
    # +--------------+-------------+----------+------------------+--------------------------+-------------------------+
    # |A16CZRQL23NOIW|Movies_and_TV| 926      |12136.959965575486| 0.9935622317596566       | 0.9944073110523517      |
    # |A16CZRQL23NOIW|Digital_Music| 6        |68.25999879837036 | 0.006437768240343348     | 0.005592688947648...    |
    out_Most_Item = 'Most_Item'
    PersonOrder.write.json(out_Most_Item, mode='overwrite')

    # person who Spend Most Money
    Top_Spend = selectTop_num.groupby('reviewer_id').sum('price')
    Top_Spend = Top_Spend.orderBy('sum(price)',ascending=False)
    #Top_Spend.show()
    SelectTopPrice = 1
    Top_Price = Top_Spend.limit(SelectTopPrice).select('reviewer_id')
    #Top_Price.show()
    PersonOrder1 = review.join(Top_Price,'reviewer_id')
    #PersonOrder1.show()
    PersonOrder1 = PersonOrder1.select('reviewer_id', 'category','price')
    PersonOrder1 = PersonOrder1.groupby('reviewer_id', 'category').sum('price')
    #PersonOrder1.show()

    sumOfPrice1 = PersonOrder1.groupby('reviewer_id').sum().collect()[0][1]
    print(sumOfPrice1)
    PersonOrder1 = PersonOrder1.withColumn('Percentage of Money_Spend', PersonOrder1['sum(price)'] / sumOfPrice1 * 100)
    PersonOrder1.show()
    # +--------------+-------------+------------------+-------------------------+
    # | reviewer_id  | category    | sum(price)       |Percentage of Money_Spend|
    # +--------------+-------------+------------------+-------------------------+
    # |A16CZRQL23NOIW|Movies_and_TV| 12136.959486     | 99.44073110523517       |
    # |A16CZRQL23NOIW|Digital_Music| 68.259997036     | 0.559268894764832       |
    # +--------------+-------------+------------------+-------------------------+

    out_Most_Spend = 'Most_Spend'
    PersonOrder1.write.json(out_Most_Spend, mode='overwrite')


    #Person who have most different order Number for each category
    #selectTop_num.show()
    id_category_orderTimes = selectTop_num.groupby('reviewer_id', 'category').sum('count')
    id_category_orderTimes = id_category_orderTimes.withColumn('categoryNum', lit(1))
    id_category_orderTimes = id_category_orderTimes.groupby('reviewer_id').sum('categoryNum')
    id_category_orderTimes = id_category_orderTimes.orderBy('sum(categoryNum)',ascending=False)
    Select_ID = id_category_orderTimes.limit(1).select('reviewer_id')

    joinedTable = review.join(Select_ID,'reviewer_id')
    joinedTable = joinedTable.withColumn('CategoryTotal', lit(1))
    joinedTable = joinedTable.groupby('reviewer_id', 'category').sum('price', 'CategoryTotal')
    joinedTable.show()
    sumOfMoney = joinedTable.groupby('reviewer_id').sum().collect()[0][1]
    sumOfCategory = joinedTable.groupby('reviewer_id').sum().collect()[0][2]
    print(sumOfMoney)
    joinedTable = joinedTable.withColumn('Category%', joinedTable['sum(CategoryTotal)'] / sumOfCategory * 100)
    joinedTable.show()
    # +--------------+-------------------+------------------+------------------+------------------+
    # |  reviewer_id | category          | sum(price)       |sum(CategoryTotal)|       Category % |
    # +--------------+-------------------+------------------+------------------+------------------+
    # |A356RFKNIG043B|     Digital_Music | 3.990000009536743| 1                |1.1111111111111112|
    # |A356RFKNIG043B|     Movies_and_TV | 790.4100003838539| 76               | 84.44444444444444|
    # |A356RFKNIG043B|Musical_Instruments|182.59000062942505| 6                | 6.666666666666667|
    # |A356RFKNIG043B|              Baby | 1603.029987335205| 7                | 7.777777777777778|
    # +--------------+-------------------+------------------+------------------+------------------+

    out_Most_Different = 'Most_category'
    joinedTable.write.json(out_Most_Different, mode='overwrite')


if __name__ == '__main__':
    spark = SparkSession.builder.appName('topPrecentage').getOrCreate()
    assert spark.version >= '2.4'
    spark.sparkContext.setLogLevel('WARN')
    inputs = sys.argv[1]

    main(inputs)

