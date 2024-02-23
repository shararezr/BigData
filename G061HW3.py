from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark import StorageLevel
import threading
import sys
import random
import statistics

# Input parameters
D = int(sys.argv[1])
W = int(sys.argv[2])
left = int(sys.argv[3])
right = int(sys.argv[4])
K = int(sys.argv[5])
portExp = int(sys.argv[6])

# After how many items should we stop?
THRESHOLD = 10000000

# Count sketch data structure
count_sketch = [[0 for j in range(W)] for i in range(D)]
hash_functions = []
g_hash = []

# Generate D hash functions
p = 8191
for i in range(D):
    a = random.randint(1, p-1)
    b = random.randint(0, p-1)
    hash_functions.append(lambda x, a=a, b=b: ((a * x + b) % p) % W)
    g_hash.append(lambda x, a=a, b=b: ((a * x + b) % p) % 2  -  bool(1-((a * x + b) % p) % 2) )

# Operations to perform after receiving an RDD 'batch' at time 'time'
def process_batch(time, batch):
    # We are working on the batch at time `time`.
    global streamLength, histogram, count_sketch
    batch_size = batch.count()
    streamLength[0] += batch_size

    # Filter items in the specified interval
    batch = batch.filter(lambda x: left <= int(x) <= right)

    # Extract the distinct items from the batch
    batch_items = batch.map(lambda s: (int(s), 1)).reduceByKey(lambda i1, i2: i1 + i2).collectAsMap()



    # Update the streaming state
    for key, value in batch_items.items():
        if key not in histogram:
            histogram[key] = value
        else:
            histogram[key] += value

        # Update the count sketch
        for i in range(D):
            j = hash_functions[i](key)
            g = g_hash[i](key)
            count_sketch[i][j] += value*g

    # If we wanted, here we could run some additional code on the global histogram
    if batch_size > 0:
        print("Batch size at time [{0}] is: {1}".format(time, batch_size))

    if streamLength[0] >= THRESHOLD:
        stopping_condition.set()

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[*]").setAppName("HW3_061")
    conf = conf.set("spark.executor.memory", "4g").set("spark.driver.memory", "4g")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 1) # Batch duration of 1 second
    ssc.sparkContext.setLogLevel("ERROR")

    stopping_condition = threading.Event()

    print("Receiving data from port =", portExp)

    streamLength = [0] # Stream length (an array to be passed by reference)
    histogram = {} # Hash Table for the distinct elements

    stream = ssc.socketTextStream("algo.dei.unipd.it", portExp, StorageLevel.MEMORY_AND_DISK)

    # Filter items in the specified interval
    #streamR = stream.filter(lambda x: left <= int(x) <= right)
    streamR = stream

    # Process the filtered stream
    streamR.foreachRDD(lambda time, batch: process_batch(time, batch))

    print("Starting streaming engine")
    ssc.start()

    print("Waiting for shutdown condition")
    stopping_condition.wait()

    print("Stopping the streaming engine")
    ssc.stop(False, True)

    print("Streaming engine stopped")

total_items = streamLength[0]

total_items_in_interval = sum(histogram.values())

distinct = len(histogram)

# Compute the true second moment F2 of ΣR
F2 = sum([freq ** 2 for freq in histogram.values()]) / (total_items_in_interval ** 2)

# Compute the approximate second moment F~2 of ΣR using count sketch
F2_est = []
for i in range(D):
    est = 0
    for j in range(W):
        est += count_sketch[i][j] ** 2
    F2_est.append(est)
F2_est = statistics.median(F2_est)
F2_est /= (total_items_in_interval ** 2)

# Compute the average relative error of the frequency estimates provided by the count sketch

sorted_histogram = sorted(histogram.items(), key=lambda x: x[1], reverse=True)[:K]
phi_K = sorted_histogram[K - 1][1]

avg_err = 0.0
num_items = 0

for item, true_freq in sorted_histogram:
    if true_freq >= phi_K:
        est_freqs = []
        for i in range(D):
            g = g_hash[i](item)
            j = hash_functions[i](item)
            est_freqs.append(g*count_sketch[i][j])
        est_freq = statistics.median(est_freqs)
        avg_err += abs(true_freq - est_freq) / true_freq
        num_items += 1

if num_items > 0:
    avg_err /= num_items

# Print final statistics
print("D =", D)
print("W =", W)
print("[left,right] =", [left, right])
print("K =", K)
print("Port =", portExp)
print("Total number of items =", total_items)
print("Total number of items in [{0},{1}] = {2}".format(left, right, total_items_in_interval))

distinct = len(histogram)
print("Number of distinct items in [{0},{1}] = {2}".format(left, right, distinct))
cnt= 0
err = 0.0
numItems = 0

if K <= 20:
    sorted_histogram = sorted(histogram.items(), key=lambda x: x[1], reverse=True)[:K]
    for item, true_freq in sorted_histogram :
        est_freqs = []
        cnt += 1
        for i in range(D):
            j = hash_functions[i](item)
            g = g_hash[i](item)
            est_freqs.append(g*count_sketch[i][j])
        est_freq = statistics.median(est_freqs)
        print("Item{0}:{1} True Freq = {2} Est. Freq = {3}".format(cnt,item, true_freq, est_freq))

print("Avg err for top {0} = {1}".format(K, avg_err))

print("F2 {0} F2 Estimate {1}".format(F2, F2_est))