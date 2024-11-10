# Distributed Triangle Counting and Approximate Frequency Estimation with Spark

![Python](https://img.shields.io/badge/language-python-blue.svg)
![PyTorch](https://img.shields.io/badge/framework-PyTorch-orange.svg)

This repository contains multiple projects that implement various algorithms for distributed graph processing and real-time data analysis using **Apache Spark** and **Spark Streaming**. The main focus of these projects is on **triangle counting** in large-scale graphs and **frequency estimation** of items in a data stream.

## Table of Contents
- [Project 1: Distributed Triangle Counting](#project-1-distributed-triangle-counting)
- [Project 2: MapReduce Triangle Counting](#project-2-mapreduce-triangle-counting)
- [Project 3: Real-Time Frequency Estimation with Count Sketch](#project-3-real-time-frequency-estimation-with-count-sketch)
- [Installation](#installation)
- [Usage](#usage)
- [License](#license)

---

## Project 1: Distributed Triangle Counting

In this project, we implement two algorithms for approximating triangle counting in a **distributed computing environment** using **Apache Spark**.

### Algorithms:
1. **MR_ApproxTCwithNodeColors**: 
   - Utilizes a **MapReduce approach** combined with **node coloring** to approximate the number of triangles in a graph.
   
2. **MR_ApproxTCwithSparkPartitions**: 
   - Leverages **Spark partitions** for the same task, providing an alternative approximation method for triangle counting.

### Features:
- The project takes command-line parameters, including:
  - **C**: Number of colors
  - **R**: Number of repetitions
  - **Graph File**: File containing the graph data
- Both algorithms approximate triangle counts and output:
  - **Number of Triangles**
  - **Running Time** for each approach

This project demonstrates how **Spark** can be used for **distributed triangle counting**, providing insights into two different approximation techniques based on **node coloring** and **Spark partitions**.

---

## Project 2: MapReduce Triangle Counting with Approximation

This project implements two algorithms for **triangle counting** in large-scale graphs distributed across a cluster of machines using the **MapReduce** paradigm.

### Algorithms:
1. **Counting Triangles Algorithm**:
   - Calculates the exact number of triangles by iterating over each vertex and its neighbors to identify common neighbors.
   - Efficiently stores the neighbors using a default data structure to avoid redundant calculations.
   
2. **MapReduce Approximation Algorithm**:
   - Uses the **Count Sketch** data structure for **approximate triangle counting**.
   - Maps edges to hash codes using a hash function, groups them based on these hash codes, and reduces the data to compute triangle counts.

### Features:
- Main function sets up the **Spark environment**, reads input data, and runs either the exact or approximate algorithm based on a specified flag.
- Measures and reports:
  - **Execution time**
  - **Triangle count results**

This project provides efficient and scalable solutions for triangle counting in **large graphs** using the **MapReduce framework** in **Spark**.

---

## Project 3: Real-Time Frequency Estimation with Count Sketch

This project implements a **Spark Streaming application** that utilizes the **Count Sketch algorithm** for approximate frequency estimation of items in a real-time data stream.

### Components:
- **Initialization**:
  - Configures parameters such as the **number of hash functions**, **hash table size**, and the **range of values** to track.
  
- **Count Sketch**:
  - Uses the **Count Sketch algorithm** to maintain memory-efficient frequency estimates of items.
  - Continuously processes each batch of streaming data to update the Count Sketch data structure.

- **Real-Time Processing**:
  - The application processes real-time data from a specified port, updating frequency estimates with each batch.
  
- **Termination and Statistics**:
  - Terminates based on a predefined threshold and computes:
    - Total number of items
    - Number of distinct items
    - Average relative error of frequency estimates

This project demonstrates the use of **Spark Streaming** for real-time data processing and the **Count Sketch algorithm** for efficient, approximate frequency estimation in streams.

---

## Installation

To set up and run the projects locally, follow these steps:

1. Clone the repository:

   ```bash
   git clone https://github.com/yourusername/triangle-counting-spark.git
   cd triangle-counting-spark
