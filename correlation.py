import json
from pyspark import SparkConf, SparkContext
import sys
from pyspark.sql import SparkSession, types
from pyspark.sql import functions as f
import csv
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+


def get_cate_list(inputs):
    schema = types.StructType([
        types.StructField('category', types.StringType()),
        types.StructField('date_time', types.DateType())
    ])
    data = spark.read.json(inputs, schema=schema)
    cate_list = data.select('category').drop_duplicates(['category']).rdd.flatMap(lambda x: x).collect()
    print(cate_list)
    data = data.withColumn('month', f.month('date_time'))
    return data, cate_list


def gather_df(data, cate_list):
    global gather_data
    i = 0
    # gather_data for all categories
    for cate in cate_list:
        single_cate_data = data.where(data['category'] == cate).groupBy('month').agg(f.count('month').alias(cate)). \
            orderBy('month')
        # single_cate_data.show()
        if i == 0:
            gather_data = single_cate_data
        else:
            gather_data = gather_data.join(single_cate_data, 'month').orderBy('month')
            gather_data.cache()
        i = i + 1
        gather_data.show()
    return gather_data


def correlations_1_to_n(df, cate_list):
    df.cache()
    with open('correlation_data.csv', 'w') as writeFile:
        for c in cate_list:
            writeFile.write(c + ',')
        writeFile.write("\n")
    writeFile.close()
    category_dict = {}
    for cate in cate_list:
        category_dict[cate] = []

    for category in cate_list:
        for cate in cate_list:
            if cate != category:
                corr = df.agg(f.corr(df[category], df[cate])).collect()[0][0]
                print("correlation between {} and {} is {}".format(category, cate, corr))
                category_dict[category].append(corr)
        with open('correlation_data.csv', 'a') as writeFile:
            writeFile.write(category + ',')
            for c in category_dict[category]:
                writeFile.write(str(round(c, 4)) + ',')
            writeFile.write("\n")
    writeFile.close()
    # print(category_dict)


if __name__ == '__main__':
    spark = SparkSession.builder.appName('correlation').getOrCreate()
    assert spark.version >= '2.4'
    spark.sparkContext.setLogLevel('WARN')
    if len(sys.argv) != 2:
        print("Invalid argument")
        print("Usage:\ncorrelation.py [input]")
        exit(0)
    inputs = sys.argv[1]
    data, cate_list = get_cate_list(inputs)
    df = gather_df(data, cate_list)
    correlations_1_to_n(df, cate_list)

