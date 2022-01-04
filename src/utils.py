def setup_logs_and_monitoring(log_level: str):
    pass


def batch(iterable, n=1000):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx: min(ndx + n, l)]