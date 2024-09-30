![Python](https://img.shields.io/badge/language-python-blue.svg)

Project 1:

The provided Project implements two algorithms for approximating triangle counting in a distributed computing environment using Spark. 
The first algorithm, MR_ApproxTCwithNodeColors, utilizes a map-reduce approach with node coloring to approximate the number of triangles in a graph. 
The second algorithm, MR_ApproxTCwithSparkPartitions, leverages Spark partitions for the same purpose.

The main function reads command-line parameters, including the number of colors (C), the number of repetitions (R), and the file name containing the graph data. 
It then performs triangle counting using both algorithms and prints the results, including the number of triangles and the running time for each approach.
Overall, this Project demonstrates the use of Spark for distributed triangle counting, offering two different approximation methods based on node coloring and Spark partitions.



Project 2:

This Project is a Spark application that implements two algorithms for counting triangles in a graph using the MapReduce paradigm. 
The algorithms are designed to handle large-scale graphs distributed across a cluster of machines.
The main components of the Project includes:

Counting Triangles Algorithm: This algorithm calculates the total number of triangles in the graph by iterating over each vertex and its neighbors to identify triangles. It uses a default data structure to efficiently store the neighbors of each vertex and counts triangles by checking for common neighbors.

MapReduce Approximation Algorithm: 
This algorithm employs the Count Sketch data structure for approximate triangle counting. It first maps each edge to a hash code using a hash function, then reduces the data by grouping edges based on their hash codes, and finally computes triangle counts for each group.

Main Function: The main function sets up the Spark environment, reads input data from a file, and executes either the exact or approximate triangle counting algorithm based on a specified flag. It also measures and reports the execution time and results of the algorithm.
Overall, this Project provides efficient and scalable solutions for triangle counting in large graphs using the MapReduce framework provided by Spark.


Project 3:

This Project is a Spark Streaming application that implements the Count Sketch algorithm for approximate frequency estimation of items in a stream. The program receives a stream of data from a specified port and processes each batch of data in real-time.

The main components of the project includes:

Initialization of parameters such as the number of hash functions, hash tables size, and the range of values of interest.
Generation of hash functions for the Count Sketch algorithm.
Processing of each batch of data to update the Count Sketch data structure and compute frequency estimates for distinct items.
Termination condition based on a predefined threshold.
Final computation of statistics including total number of items, number of distinct items, and average relative error of frequency estimates.
we utilize Spark Streaming to handle real-time data processing, and the Count Sketch algorithm for memory-efficient approximate frequency estimation. Additionally, it computes statistics such as the average relative error and approximate second moment of the data stream.
