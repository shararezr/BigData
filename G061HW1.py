from pyspark import SparkContext, SparkConf
import sys
import os
import random as rand
import time
from collections import defaultdict
import statistics


def hC(edge,a, b, C):
    p = 8191
    vertex = tuple(map(int, edge.split(",")))
    h1 = ((a*vertex[0] + b) % p) % C
    h2 = ((a*vertex[1] + b) % p) % C

    if h1 == h2:
        return (h1,vertex)
    else:
        return (C+1,vertex)


def RDDTransformation(edge):
    edges = []
    for e in edge:
        ed = tuple(map(int, e.split(",")))
        edges.append(ed)

    return edges


def CountTriangles(edges):
    # Create a defaultdict to store the neighbors of each vertex
    neighbors = defaultdict(set)
    for edge in edges:
        u, v = edge[0],edge[1]
        neighbors[u].add(v)
        neighbors[v].add(u)

    # Initialize the triangle count to zero
    triangle_count = 0

    # Iterate over each vertex in the graph.
    # To avoid duplicates, we count a triangle <u, v, w> only if u<v<w
    for u in neighbors:
        # Iterate over each pair of neighbors of u
        for v in neighbors[u]:
            if v > u:
                for w in neighbors[v]:
                    # If w is also a neighbor of u, then we have a triangle
                    if w > v and w in neighbors[u]:
                        triangle_count += 1
    # Return the total number of triangles in the graph
    return [triangle_count]


def MR_ApproxTCwithNodeColors(docs,C, a, b):
    triangle_count = (docs.map(lambda e: hC(e,a, b, C))# <-- MAP PHASE (R1)#
                      .filter(lambda x: x[0] <C)
                      .groupByKey()
                      .flatMap(lambda x :CountTriangles(x[1])) # <-- REDUCE PHASE (R1)
                      .sum())# <-- REDUCE PHASE (R2)
    return triangle_count*(C**2)



def MR_ApproxTCwithSparkPartitions(docs,C):
    triangle_count = (docs.mapPartitions(RDDTransformation)
                      .mapPartitions(CountTriangles) # <-- REDUCE PHASE (R1)
                      .sum())
    return triangle_count*(C**2)




def main():

    # CHECKING NUMBER OF CMD LINE PARAMETERS
    assert len(sys.argv) == 4, "Usage:python G061HW1.py <C> <R> <file_name>"

    # SPARK SETUP
    conf = SparkConf().setAppName('G061HW1.py').setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # INPUT READING

    # 1. Read parameters
    C = sys.argv[1]
    assert C.isdigit(), "C must be an integer"
    C = int(C)

    R = sys.argv[2]
    assert R.isdigit(), "R must be an integer"
    R = int(R)



    # 2. Read input file and subdivide it into K random partitions
    data_path = sys.argv[3]
    assert os.path.isfile(data_path), "File or folder not found"
    rawData = sc.textFile(data_path).repartition(C).cache()


    t_final_c = []
    AvgRunTime = 0
    for i in range(R):
        start = time.time()
        p = 8191
        a,b = rand.randint(1, p-1), rand.randint(0, p-1)
        t_final_c.append(MR_ApproxTCwithNodeColors(rawData,C,a,b))
        end = time.time()
        AvgRunTime = AvgRunTime + (end-start)

    avgRunTime = AvgRunTime/R
    avgTriangleC = statistics.median(t_final_c)
    start = time.time()
    t_final = MR_ApproxTCwithSparkPartitions(rawData,C)
    end = time.time()
    runingtime = end - start

    print("Dataset =",data_path )
    print("Number of Edges =", rawData.count())
    print("Number of Colors =", C)
    print("Number of Repetitions =", R)
    print("Approximation through node coloring")
    print(f"_ Number of triangles (median over {R} runs) =", avgTriangleC)
    print(f"_ Running time (average over {R} runs) = {round(avgRunTime*(10**3))} ms")
    print("Approximation through Spark partitions")
    print("_ Number of triangles =", t_final)
    print(f"_ Running time = {round(runingtime*(10**3))} ms")






if __name__ == "__main__":
    main()

