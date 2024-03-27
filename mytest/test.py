import ray
import os
import time

ray.init(_plasma_directory = "/tmp/ray/")

# Define the square task.
@ray.remote
def square(x):
    os.makedirs("/tmp/ray/test", exist_ok=True)
    return x * x

# Launch four parallel square tasks.
futures = [square.remote(i) for i in range(4)]

# Retrieve results.
print(ray.get(futures))
# -> [0, 1, 4, 9]