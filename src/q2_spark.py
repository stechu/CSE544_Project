from pyspark import SparkContext
import csv

twitter_bucket = """s3n://bdbenchmark-data/twitter_2m.csv"""

if __name__ == "__main__":
    SparkContext.setSystemProperty('spark.executor.memory', '2500m')
    sc = SparkContext(appName="KCORE_APPLICATION")
    parallism = 16
    k = 10

    # read data from s3
    twitter = sc.textFile(twitter_bucket, parallism)
    twitter = twitter.map(lambda x: x.split(",")).map(
        lambda x: (int(x[0]), int(x[1])))

    degrees = twitter.map(lambda x, y: (x, 1)).reduceByKey(lambda a, b: a+b)
    remained = degrees.filter(lambda v, c: True if c >= k else False)
    vcount = degrees.count()
    rcount = remained.count()

    while vcount > rcount:
        twitter = twitter.join(remained).map(
            lambda (v, (u, d)): (v, u)).coalesce(parallism)
        degrees = twitter.map(
            lambda x, y: (x, 1)).reduceByKey(lambda a, b: a+b)
        remained = degrees.filter(lambda v, c: True if c >= k else False)
        vcount = degrees.count()
        rcount = remained.count()

    with open("twitter_10core.csv", 'wb') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(twitter)
