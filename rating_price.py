import json
from pyspark import SparkConf, SparkContext
import sys
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import desc
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

def price_devition(df):
    item_price_max = df.agg({"price": "max"}).collect()[0][0]
    item_price_min = df.agg({"price": "min"}).collect()[0][0]
    price_devition = (item_price_max - item_price_min) * 0.75 + item_price_min
    return price_devition

def single_category(inputs, output):
    combine_schema = types.StructType([
        types.StructField('asin', types.StringType()),
        types.StructField('overall', types.FloatType()),
        types.StructField('price', types.FloatType())
    ])

    combine = spark.read.json(inputs, schema=combine_schema)
    price = combine.select(combine['asin'].alias('item_id'),
                           combine['overall'].alias('rating'),
                           combine['price'])

    item_count = price.groupBy('item_id').count()
    item_rating_avg = price.groupBy('item_id').avg('rating').withColumnRenamed('avg(rating)', 'rating')
    item_price_avg = price.groupBy('item_id').avg('price').withColumnRenamed('avg(price)', 'price')
    rating_price = item_count.join(item_rating_avg, 'item_id')
    rating_price = rating_price.join(item_price_avg, 'item_id').orderBy('price', ascending=False)
    #rating_price.show()

    # devide low & high level price
    item_price_max = rating_price.agg({"price":"max"}).collect()[0][0]
    item_price_min = rating_price.agg({"price":"min"}).collect()[0][0]
    price_devition = (item_price_max - item_price_min) * 0.75 + item_price_min
    high_price_item = rating_price.where(rating_price.price > price_devition).orderBy('price', ascending=False)
    low_price_item = rating_price.where(rating_price.price < price_devition).orderBy('price', ascending=False)

    #high_price_item.show()
    #low_price_item.show()

    rating_price.write.csv(output1, mode='overwrite')
    high_price_item.write.csv(output2, mode='overwrite')
    low_price_item.write.csv(output3,  mode='overwrite')

def all_category(inputs, output):
    combine_schema = types.StructType([
        types.StructField('asin', types.StringType()),
        types.StructField('overall', types.FloatType()),
        types.StructField('price', types.FloatType()),
        types.StructField('category', types.StringType())
    ])

    combine = spark.read.json(inputs, schema=combine_schema)
    price = combine.select(combine['asin'].alias('item_id'),
                           combine['overall'].alias('rating'),
                           combine['price'],
                           combine['category'])

    category_count = price.groupBy('category').count()
    category_count.show()
    category_rating_avg = price.groupBy('category').avg('rating').withColumnRenamed('avg(rating)', 'rating')
    category_rating_avg.show()
    category_price_avg = price.groupBy('category').avg('price').withColumnRenamed('avg(price)', 'price')
    category_price_avg.show()
    rating_price = category_count.join(category_rating_avg, 'category')
    rating_price = rating_price.join(category_price_avg, 'category').orderBy('price', ascending=False)

    high_price_category = rating_price.where(rating_price.price > price_devition(rating_price)).orderBy('price', ascending=False)
    low_price_category = rating_price.where(rating_price.price < price_devition(rating_price)).orderBy('price', ascending=False)

    #high_price_category.show()
    rating_price.write.csv(output1, mode='overwrite')
    high_price_category.write.csv(output2, mode='overwrite')
    low_price_category.write.csv(output3, mode='overwrite')


if __name__ == '__main__':
    spark = SparkSession.builder.appName('ETL').getOrCreate()
    assert spark.version >= '2.4'
    spark.sparkContext.setLogLevel('WARN')
    mode = sys.argv[1]
    inputs = sys.argv[2]
    output = sys.argv[3]
    if len(sys.argv) != 4:
        print("Invalid argument")
        print("Usage:\nrating_price.py [single/all] [input] [output]")
        exit(0)
    if mode == 'single':
        single_category(inputs, output)
    elif mode == 'all':
        all_category(inputs, output)
    else:
        print("Invalid argument")
        print("Usage:\nrating_price.py [single/all] [input] [output]")
        exit(0)



