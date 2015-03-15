from pyspark import SparkContext

# particle of interest
# nowGroup:long, iOrder:long, mass:float, hi: float, type:string,
# timeStep: long, grp: long
poi_bucket = """s3n://bdbenchmark-data/poi.csv"""

# halo table
# nowGroup:long, grpID:long, timeStep:long, mass:double, numParticles: long
# HI: double
halo_bucket = """s3n://bdbenchmark-data/halo.csv"""


if __name__ == "__main__":
    SparkContext.setSystemProperty('spark.executor.memory', '3500m')
    sc = SparkContext(appName="MTREE_APPLICATION")
    parallism = 16

    # read data from s3
    poi = sc.textFile(poi_bucket, parallism)
    poi = poi.map(lambda x: x.split(",")).map(
        lambda x: (long(x[0]), long(x[1]), float(x[2]),
                   float(x[3]), str(x[4]), long(x[5]), long(x[6])))

    halo = sc.textFile(poi_bucket, parallism)
    halo = halo.map(lambda x: x.split(",")).map(
        lambda x: (long(x[0]), long(x[1]), long(x[2]), float(x[3]),
                   long(x[4]), float(x[5])))

    # filter halo table
    halo = halo.filter(lambda x: True if x[4] > 256 else False)

    # join halo table with poi table
    # after transformation: (nowGrp, grpId, timeStamp), (mass, numP, HI)
    halo = halo.map(lambda x: ((x[0], x[1], x[2]), (x[3], x[4], x[5])))
    # after transformation: (nowGrp, grpId, timeStamp), (iOrder)
    poi = poi.map(lambda x: ((x[0], x[6], x[5]), (x[1])))
    particles = poi.join(halo).map(
        lambda (x, (a, b)): (x, b))
    print particles.take(1)

