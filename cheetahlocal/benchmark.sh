#!/bin/bash
set -e

NUMBER_OF_RUNS=5
NUMBER_OF_THREADS=6
THREAD_COUNT=(1 2 4 8 16)

function benchmark() {
    local bench_name="$1"
    local command="$2"

    local num_threads="0"

    echo "---------------------"
    echo "Running:   ${bench_name}"
    echo "Executing: ${command}"
    echo "---------------------"


    # while [[ ${num_threads} -lt ${NUMBER_OF_THREADS} ]]; do
    for num_threads in ${THREAD_COUNT[@]}; do
        # num_threads=$[${num_threads} + 1]

        echo "Thread(s) Used: ${num_threads}"
        local total_runtime="0"

        local counter="0"
        while [[ ${counter} -lt ${NUMBER_OF_RUNS} ]]; do
            counter=$[${counter} + 1]

            local temp_file=$(mktemp)
            echo "${num_threads}" | ${command} > ${temp_file}
            local runtime=$(tac ${temp_file} |egrep -m 1 .)
            rm ${temp_file}

            # echo -e "Run trial: ${counter}\tRuntime: ${runtime}"

            total_runtime=$[${total_runtime} + ${runtime}]
        done

        echo "Average Runtime: $[${total_runtime} / ${NUMBER_OF_RUNS}]"
        echo ""

    done
}

benchmark "dense coarse lock" "./run SimpleQueryExecutor NewColStoreEngParallel ./Sample_Input_Files/nobench_data.json ./Sample_Input_Files/insert_query_Contention_500 1 CoarseLock"
benchmark "dense fine lock" "./run SimpleQueryExecutor NewColStoreEngParallel ./Sample_Input_Files/nobench_data.json ./Sample_Input_Files/insert_query_Contention_500 1 FineLock"

benchmark "mixed coarse lock" "./run SimpleQueryExecutor NewColStoreEngParallel ./Sample_Input_Files/nobench_data.json ./Sample_Input_Files/workload_50select_50insert 1 FineLock"
benchmark "mixed fine lock" "./run SimpleQueryExecutor NewColStoreEngParallel ./Sample_Input_Files/nobench_data.json ./Sample_Input_Files/workload_50select_50insert 1 FineReadWriteLock"

benchmark "mixed with 80% select coarse lock" "./run SimpleQueryExecutor NewColStoreEngParallel ./Sample_Input_Files/nobench_data.json ./Sample_Input_Files/workload_80select_20insert 1 CoarseReadWriteLock"
benchmark "mixed with 80% select fine lock" "./run SimpleQueryExecutor NewColStoreEngParallel ./Sample_Input_Files/nobench_data.json ./Sample_Input_Files/workload_80select_20insert 1 FineReadWriteLock"


# benchmark "No lock" "./run SimpleQueryExecutor NewColStoreEngParallel ./Sample_Input_Files/nobench_data.json ./Sample_Input_Files/insert_query 1"
# benchmark "No lock with 20 inserts and 80 selects" "./run SimpleQueryExecutor NewColStoreEngParallel ./Sample_Input_Files/workload_80select_20insert ./Sample_Input_Files/insert_query 1"
# benchmark "No lock with 50 inserts and 50 selects" "./run SimpleQueryExecutor NewColStoreEngParallel ./Sample_Input_Files/workload_50select_50insert ./Sample_Input_Files/insert_query 1"
# benchmark "No lock with 80 inserts and 20 selects" "./run SimpleQueryExecutor NewColStoreEngParallel ./Sample_Input_Files/workload_20select_80insert ./Sample_Input_Files/insert_query 1"

# benchmark "Single Lock" "./run SimpleQueryExecutor NewColStoreEngParallel ./Sample_Input_Files/nobench_data.json ./Sample_Input_Files/insert_query 1 Coarse"
# benchmark "Single Lock with 20 inserts and 80 selects" "./run SimpleQueryExecutor NewColStoreEngParallel ./Sample_Input_Files/workload_80select_20insert ./Sample_Input_Files/insert_query 1 Coarse"
# benchmark "Single Lock with 50 inserts and 50 selects" "./run SimpleQueryExecutor NewColStoreEngParallel ./Sample_Input_Files/workload_50select_50insert ./Sample_Input_Files/insert_query 1 Coarse"
# benchmark "Single Lock with 80 inserts and 20 selects" "./run SimpleQueryExecutor NewColStoreEngParallel ./Sample_Input_Files/workload_20select_80insert ./Sample_Input_Files/insert_query 1 Coarse"


# benchmark "Lock per column" "./run SimpleQueryExecutor NewColStoreEngParallel ./Sample_Input_Files/nobench_data.json ./Sample_Input_Files/insert_query 1 Fine"
# benchmark "Lock per column with 20 inserts and 80 selects" "./run SimpleQueryExecutor NewColStoreEngParallel ./Sample_Input_Files/workload_80select_20insert ./Sample_Input_Files/insert_query 1 Fine"
# benchmark "Lock per column with 50 inserts and 50 selects" "./run SimpleQueryExecutor NewColStoreEngParallel ./Sample_Input_Files/workload_50select_50insert ./Sample_Input_Files/insert_query 1 Fine"
# benchmark "Lock per column with 80 inserts and 20 selects" "./run SimpleQueryExecutor NewColStoreEngParallel ./Sample_Input_Files/workload_20select_80insert ./Sample_Input_Files/insert_query 1 Fine"
