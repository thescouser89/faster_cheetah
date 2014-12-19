# Cheetah
Cheetah is an in-memory database written in Java. The main developer of Cheetah
is Alan Lu, who is one of the masters student of Professor Amza at the
University of Toronto.

Our project consists of making the runtime of queries faster by using threads.
This repository is the code that we modified to achieve this.


# How to run

### ColStore

```
cd cheetahlocal
# nobench_data.json is our sample JSON data to insert
# sample_query_8_25 is the queries we'll run in Cheetah
./run SimpleQueryExecutor NewColStoreEng ./Sample_Input_Files/nobench_data.json ./Sample_Input_Files/sample_query_8_25 1
```

### RowStore

```
cd cheetahlocal
# nobench_data.json.def is the schema we'll use for the table
./run SimpleQueryExecutor NewRowStoreEng ./Sample_Input_Files/nobench_data.json ./Sample_Input_Files/sample_query_8_25 1 ./Sample_Input_Files/nobench_data.json.def
```

# RowColStore

```
cd cheetahlocal
# sample_layout is hte schema for all the tables we'll create
./run SimpleQueryExecutor NewRowColStoreEng ./Sample_Input_Files/nobench_data.json ./Sample_Input_Files/sample_query_8_25 1 ./Sample_Input_Files/sample_layout
```

## Our Project Modifications run

### ColStoreParallel

```
cd cheetahlocal
./run SimpleQueryExecutor NewColStoreEngParallel ./Sample_Input_Files/nobench_data.json ./Sample_Input_Files/sample_query_8_25 1
```

### ColStoreParallel with insert queries

```
cd cheetahlocal
./run SimpleQueryExecutor NewColStoreEngParallel ./Sample_Input_Files/nobench_data.json ./Sample_Input_Files/insert_query 1
```
