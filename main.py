import csv
import json
import multiprocessing
import random
import time

from src.map_reduce import MapReduce
from src.map_reduce_functions import (
    calculate_gpa,
    map_by_semester,
    map_by_studentid,
    reduce_by_studentid,
)
from src.read_csv import get_input_from_csv_dir

if __name__ == "__main__":
    print("Getting Input...")
    input_records = get_input_from_csv_dir("example_data")
    print(f"{len(input_records)} total records...\n")

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    #   SEQUENTIAL TESTING
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    NUM_TRIALS: int = 5
    sequential_times = []

    for i in range(NUM_TRIALS):
        random.seed(5 * i + 29)

        first_stage_map_reduce = MapReduce(
            mapper=map_by_semester, reducer=calculate_gpa
        )
        second_stage_map_reduce = MapReduce(
            mapper=map_by_studentid, reducer=reduce_by_studentid
        )
        print(f"Iteration #{i}")
        sequential_start_time = time.time()
        # ~~~~ Start Timer ~~~ #

        # ~~~~ Stage 1 ~~~ #
        sequential_gpa_data = first_stage_map_reduce.sequential(input_records)

        # ~~~~ Stage 2 ~~~ #
        sequential_result = second_stage_map_reduce.sequential(
            sequential_gpa_data
        )

        # ~~~~ End Timer ~~~ #
        sequential_end_time = time.time()
        sequential_time = sequential_end_time - sequential_start_time
        print(f"Sequential Execution Time {sequential_time:.3f} seconds.\n")

        sequential_times.append(sequential_time)
    avg_sequential_run_time: float = sum(sequential_times) / len(
        sequential_times
    )
    print(f"Average time for sequential MapReduce: {avg_sequential_run_time}")

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    #   PARALLEL TESTING
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    NUM_TRIALS = 5
    max_num_workers: int = multiprocessing.cpu_count() * 2
    chunks_per_process: int = 4

    print(f"Max number of Workers: {max_num_workers}\n")

    parallel_times: dict[int, list[float]] = {}
    for num_workers in range(0, max_num_workers + 2, 4):
        num_workers = max(1, num_workers)
        # ~~~~ Prepare To Store Run-Times ~~~ #
        parallel_times[num_workers] = []
        for i in range(NUM_TRIALS):
            random.seed(5 * i + 29)
            print(f"({num_workers} workers), Iteration #{i}")
            # ~~~~ Create MapReduce Objects ~~~ #
            first_stage_map_reduce = MapReduce(
                mapper=map_by_semester,
                reducer=calculate_gpa,
                num_workers=num_workers,
            )
            second_stage_map_reduce = MapReduce(
                mapper=map_by_studentid,
                reducer=reduce_by_studentid,
                num_workers=num_workers,
            )
            parallel_start_time = time.time()
            # ~~~~ Start Timer ~~~ #

            # ~~~~ Stage 1 ~~~ #
            chunksize = max(
                1, len(input_records) // (num_workers * chunks_per_process)
            )
            parallel_gpa_data = first_stage_map_reduce.parallel(
                input_records, chunksize=chunksize
            )

            # ~~~~ Stage 2 ~~~ #
            chunksize = max(
                1, len(parallel_gpa_data) // (num_workers * chunks_per_process)
            )
            parallel_result = second_stage_map_reduce.sequential(
                parallel_gpa_data, chunksize=chunksize
            )

            # ~~~~ End Timer ~~~ #
            parallel_end_time = time.time()
            parallel_time = parallel_end_time - parallel_start_time
            parallel_times[num_workers].append(parallel_time)
            print(f"Parrallel Execution Time: {parallel_time:.3f} seconds.")

            # ~~~~ Clean-up ~~~ #
            first_stage_map_reduce.pool.terminate()
            first_stage_map_reduce.pool.join()
            second_stage_map_reduce.pool.terminate()
            second_stage_map_reduce.pool.join()
        print()

    output_csv_path: str = f"Parallel_Results_{time.time()}.csv"
    print(output_csv_path)
    with open(
        output_csv_path, mode="w", newline="", encoding="utf-8"
    ) as parallel_results_csv:
        writer = csv.writer(parallel_results_csv)
        writer.writerow(
            (f"Parallel Results on {len(input_records)} records.",)
        )
        writer.writerow(("Average Sequential Time:", avg_sequential_run_time))
        writer.writerow("")
        writer.writerow(parallel_times.keys())

        for i in range(max([len(val) for val in parallel_times.values()])):
            writer.writerow([val[i] for val in parallel_times.values()])

    print()
    print("Sequential and Parallel Results Match? ")
    matches: bool = True
    for sequential_item, parallel_item in zip(
        sorted(sequential_result, key=lambda x: x["StudentID"]),
        sorted(parallel_result, key=lambda x: x["StudentID"]),
    ):
        if sequential_item != parallel_item:
            matches = False
            print("~" * 40 + "\n" + "Items do not match:" + "\n" + "~" * 40)
            print(json.dumps(sequential_item, indent=4))
            print(json.dumps(parallel_item, indent=4))
            print("\n")
    print("\tYes" if matches else "\tNo")
