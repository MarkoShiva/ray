# __without_limit_start__
import ray

# Assume this Ray node has 16 CPUs and 16G memory.
ray.init()


@ray.remote
def process(file):
    # Actual work is reading the file and process the data.
    # Assume it needs to use 2G memory.
    pass


NUM_FILES = 1000
result_refs = [process.remote(f"{i}.csv") for i in range(NUM_FILES)]
ray.get(result_refs)
# __without_limit_end__

# __with_limit_start__
result_refs = [
    process.options(memory=2 * 1024 * 1024 * 1024).remote(f"{i}.csv")
    for i in range(NUM_FILES)
]

ray.get(result_refs)
# __with_limit_end__
