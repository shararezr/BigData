from pyspark import *
import sys
import os
import random as rand
import time
from collections import defaultdict
from statistics import median

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

def countTriangles2(colors_tuple, edges, rand_a, rand_b, p, num_colors):
    #We assume colors_tuple to be already sorted by increasing colors. Just transform in a list for simplicity
    colors = list(colors_tuple)  
    #Create a dictionary for adjacency list
    neighbors = defaultdict(set)
    #Creare a dictionary for storing node colors
    node_colors = dict()
    for edge in edges:
        u, v = edge
        node_colors[u]= ((rand_a*u+rand_b)%p)%num_colors
        node_colors[v]= ((rand_a*v+rand_b)%p)%num_colors
        neighbors[u].add(v)
        neighbors[v].add(u)

    # Initialize the triangle count to zero
    triangle_count = 0

    # Iterate over each vertex in the graph
    for v in neighbors:
        # Iterate over each pair of neighbors of v
        for u in neighbors[v]:
            if u > v:
                for w in neighbors[u]:
                    # If w is also a neighbor of v, then we have a triangle
                    if w > u and w in neighbors[v]:
                        # Sort colors by increasing values
                        triangle_colors = sorted((node_colors[u], node_colors[v], node_colors[w]))
                        # If triangle has the right colors, count it.
                        if colors==triangle_colors:
                            triangle_count += 1
    # Return the total number of triangles in the graph
    return triangle_count

#ALGORITHM1
def MR_ApproxTCwithNodeColors(docs,C, a, b):
    triangle_count = (docs.map(lambda e: hC(e,a, b, C))# <-- MAP PHASE (R1)#
                      .filter(lambda x: x[0] <C)
                      .groupByKey()
                      .flatMap(lambda x :CountTriangles(x[1])) # <-- REDUCE PHASE (R1)
                      .sum())# <-- REDUCE PHASE (R2)
    return triangle_count*(C**2)

def MR_ExactTC(edges, C, a, b, p):
    def hC(vertex):
        return ((a * vertex + b) % p) % C

    triangle_count = (edges.flatMap(lambda e: [(tuple(sorted([hC(e[0]), hC(e[1]), i])), e) for i in range(C)])
                      .groupByKey()
                      .map(lambda x: countTriangles2(x[0], x[1],a,b,p,C))
                      .sum())
    return triangle_count

#OUTPUT with parameters: 16 executors, 8 colors, 3 repetitions, flag 0, file  /data/BDC2223/orkut4M.txt
def main():
     # SPARK SETUP
    conf = SparkConf().setAppName('G061HW2.py').setMaster("local[*]")
    sc = SparkContext(conf=conf)

    C = int(sys.argv[1])
    R = int(sys.argv[2])
    F = int(sys.argv[3])
    data_path = sys.argv[4]

    edges = sc.textFile(data_path).repartition(32).cache()
    print("OUTPUT with parameters: 16 executors,", C,"colors,", R,"repetitions,", "flag", F,", file", data_path)
    print("Dataset =", data_path)
    print("Number of Edges =", edges.count())
    print("Number of Colors =", C)
    print("Number of Repetitions =", R)
    print("Flag =", F)

    p = 8191
    if F == 0:
        estimates = []
        times = []
        for i in range(R):
            a = rand.randint(1, p-1)
            b = rand.randint(0, p-1)
            start_time = time.time()
            estimates.append(MR_ApproxTCwithNodeColors(edges,C,a,b))
            times.append(time.time() - start_time)
        print("Approximation algorithm with node coloring")
        print("- Number of triangles (median over", R,"runs) =", median(estimates))
        print("- Running time (average over", R,"runs) =", sum(times)/len(times), "ms")
    else:
        times = []
        for i in range(R):
            data = sc.textFile(data_path)
            edges = data.map(lambda line: tuple(map(int, line.split(","))))

            # Repartition the RDD into 32 partitions
            edges = edges.repartition(32)

            # Cache the RDD
            edges.cache()
            a = rand.randint(1, p-1)
            b = rand.randint(0, p-1)
            start_time = time.time()
            exact_count = MR_ExactTC(edges,C,a,b,p)
            times.append(time.time() - start_time)
        print("Exact algorithm with node coloring")
        print("- Number of triangles =", exact_count)
        print("- Running time (average over", R,"runs) =", sum(times)/len(times), "ms")

    sc.stop()


if __name__ == "__main__":
    main()

