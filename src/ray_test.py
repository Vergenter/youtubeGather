import ray
import time

ray.init()


@ray.remote()
def do_some_work(x):
    time.sleep(1)  # Replace this with work you need to do.
    return x


start = time.time()
results = ray.get([do_some_work.remote(x) for x in range(4)])
print("duration =", time.time() - start)
print("results = ", results)
