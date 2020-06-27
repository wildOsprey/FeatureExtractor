import time


def print_time(method):
    def wrapper(*args, **kwargs):
        ts = time.time()
        result = method(*args, **kwargs)
        te = time.time()
        print('%r  %2.2f ms' %
            (method.__name__, (te - ts) * 1000))
        return result
    return wrapper
