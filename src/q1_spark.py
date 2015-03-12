####################################################################
# lca.py - least common ancestor file
####################################################################

import sys
from pyspark import SparkContext
import csv

cites_bucket = """s3n://bdbenchmark-data/citation_jstor.csv"""
papers_bucket = """s3n://bdbenchmark-data/paper_jstor.csv"""
seed_bucket = """s3n://bdbenchmark-data/poi.csv"""


if __name__ == "__main__":
    def filter_header(s):
        """
            filter non-digit items
        """
        return s[0].isdigit() and s[1].isdigit()

    # convert program arguments
    if len(sys.argv) != 3:
        print "lca.py takes two arguments: savedFileName, sampling_rate"
    output_file_name = sys.argv[1]
    sampling_rate = float(sys.argv[2])

    print "\n---------------[BEGINNING SPARK APPLICATION]-----------------\n"

    SparkContext.setSystemProperty('spark.executor.memory', '2500m')
    sc = SparkContext(appName="LCA_APPLICATION")
    parallism = 16

    # read data from s3
    cites = sc.textFile(cites_bucket, parallism).sample(
        False, sampling_rate, 2)
    papers = sc.textFile(papers_bucket, parallism)
    seeds = sc.textFile(seed_bucket, parallism)

    # filter the annoying header
    papers = papers.map(lambda x: x.split(",")).filter(filter_header)
    # convert type
    papers = papers.map(lambda x: (int(x[0]), int(x[1])))
    # filter cites
    cites = cites.map(
        lambda x: x.split(",")).filter(filter_header)
    edges = cites.map(lambda x: (int(x[0]), int(x[1]))).cache()

    seeds = seeds.map(lambda x: int(x))

    # compute reversed shortest path
    # distances: (vertex, (seed, distance))
    distances = seeds.map(lambda x: (x, (x, 0))).cache()
    old_count = 0L
    new_count = distances.count()
    # shorted path computation, only for interested vertices
    while old_count != new_count:
        next_step = distances.join(edges).map(
            lambda (v1, ((s, d), v2)): ((v2, s), d+1))
        dist_seed_pairs = distances.map(lambda (v, (s, d)): ((v, s), d))
        distances.unpersist()
        distances = next_step.union(dist_seed_pairs).reduceByKey(
            lambda a, b: a if a < b else b).map(
            lambda ((v, s), d): (v, (s, d))).coalesce(parallism).cache()
        next_step.unpersist()
        dist_seed_pairs.unpersist()
        old_count = new_count
        new_count = distances.count()
        print "bfs-count:"+str(new_count)

    edges.unpersist()
    distances.filter(lambda (v, (s, d)): False if v == s else False)
    print "\n-------------------- bfs finished -------------------------\n"

    def transform_accestors(e):
        """
            transform tuple to accesstor: choose the larger distance
        """
        v, ((s1, d1, y1), (s2, d2, y2)) = e
        assert y1 == y2
        return ((s1, s2), (v, max(d1, d2), y1))

    def compare_accestors(v1, v2):
        """
            communitive function for choose lowest accestor
        """
        a1, d1, y1 = v1
        a2, d2, y2 = v2
        if d1 != d2:
            return v1 if d1 < d2 else v2
        elif y1 != y2:
            return v1 if y1 < y2 else v2
        else:
            return v1 if a1 < a2 else v2

    # pd: (vertex, (seed, year, distance))
    pd = distances.join(papers).map(
        lambda (v, ((s, d), y)): (v, (s, d, y)))
    accestors = pd.join(pd).filter(
        lambda (v, ((s1, d1, y1), (s2, d2, y2))): True if s1 < s2 else False)
    lca = accestors.map(transform_accestors).reduceByKey(
        compare_accestors).collect()
    with open(output_file_name, 'wb') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(lca)
    print "\n---------------[TERMINATING SPARK APPLICATION]-----------------\n"
