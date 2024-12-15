import collections
import multiprocessing
import time


class MapReduce:
    """
    Sources:
        https://pymotw.com/2/multiprocessing/mapreduce.html
    """

    def __init__(self, mapper, reducer, num_workers=None):
        self.mapper = mapper
        self.reducer = reducer
        self.pool = multiprocessing.Pool(processes=num_workers)

        self.num_workers_ = num_workers

    def shuffle(self, mapped_records):
        shuffled_data = collections.defaultdict(list)
        for key, value in mapped_records:
            shuffled_data[key].append(value)
        return shuffled_data.items()

    def parallel(self, inputs, chunksize):
        start_time = time.time()
        map_responses = list(
            self.pool.imap_unordered(self.mapper, inputs, chunksize=chunksize)
        )
        end_time = time.time()
        print(f"\tMapping took: {(end_time - start_time):.3f} seconds.")

        start_time = time.time()
        shuffled_data = self.shuffle(map_responses)
        end_time = time.time()
        print(f"\tShuffling took: {(end_time - start_time):.3f} seconds.")

        start_time = time.time()
        reduced_values = self.pool.map(self.reducer, shuffled_data)
        end_time = time.time()
        print(f"\tReducing took: {(end_time - start_time):.3f} seconds.")

        return reduced_values

    def sequential(self, inputs, chunksize=1):
        start_time = time.time()
        map_responses = []
        for item in inputs:
            map_responses.append(self.mapper(item))
        end_time = time.time()
        print(f"\tMapping took: {(end_time - start_time):.3f} seconds.")

        start_time = time.time()
        shuffled_data = self.shuffle(map_responses)
        end_time = time.time()
        print(f"\tShuffling took: {(end_time - start_time):.3f} seconds.")

        start_time = time.time()
        reduced_values = []
        for item in shuffled_data:
            reduced_values.append(self.reducer(item))
        end_time = time.time()
        print(f"\tReducing took: {(end_time - start_time):.3f} seconds.")

        return list(reduced_values)
