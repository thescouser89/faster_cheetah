# Cheetah
Cheetah is an in-memory database written in Java. The main developer of Cheetah
is Alan Lu, who is one of the masters students of Professor Cristiana Amza at the
University of Toronto.
Our project consists of making the runtime for queries faster by using threads.
This repository is the code that we modified to achieve this.

Modifications are in cheetahlocal. No changes have been done to cheetahlocalOld.

ECE1747 group:
Gary Chaw, Dustin Kut Moy Cheung, Alan Ng

# How to run
### ColStore
```
cd cheetahlocal
make clean; make SimpleQueryExecutor
# nobench_data.json is our sample JSON data to insert
# sample_query_8_25 is the queries we'll run in Cheetah
./run SimpleQueryExecutor NewColStoreEng ./Sample_Input_Files/nobench_data.json ./Sample_Input_Files/sample_query_8_25 1
```

### RowStore
```
cd cheetahlocal
make clean; make SimpleQueryExecutor
# nobench_data.json.def is the schema we'll use for the table
./run SimpleQueryExecutor NewRowStoreEng ./Sample_Input_Files/nobench_data.json ./Sample_Input_Files/sample_query_8_25 1 ./Sample_Input_Files/nobench_data.json.def
```

### RowColStore
```
cd cheetahlocal
make clean; make SimpleQueryExecutor
# sample_layout is hte schema for all the tables we'll create
./run SimpleQueryExecutor NewRowColStoreEng ./Sample_Input_Files/nobench_data.json ./Sample_Input_Files/sample_query_8_25 1 ./Sample_Input_Files/sample_layout
```

## Our Project Modifications run
This part shows the code that needs to be run to use this program with threads.
We modified the ColStore engine to run with threads, but using a threadpool.

### ColStoreParallel

```
cd cheetahlocal
make clean; make SimpleQueryExecutor
./run SimpleQueryExecutor NewColStoreEngParallel ./Sample_Input_Files/nobench_data.json ./Sample_Input_Files/sample_query_8_25 1
```

### ColStoreParallel with insert queries
The `insert_query` file is used to do both queries and inserts into the
database. The `INSERT` statement is a new feature that we added to observe the
performance behaviour for a mix-strategy query.

```
cd cheetahlocal
make clean; make SimpleQueryExecutor
./run SimpleQueryExecutor NewColStoreEngParallel ./Sample_Input_Files/nobench_data.json ./Sample_Input_Files/insert_query_Contention_500 1 CoarseLock
./run SimpleQueryExecutor NewColStoreEngParallel ./Sample_Input_Files/nobench_data.json ./Sample_Input_Files/insert_query_Contention_500 1 FineLock
./run SimpleQueryExecutor NewColStoreEngParallel ./Sample_Input_Files/nobench_data.json ./Sample_Input_Files/workload_50select_50insert 1 FineLock
./run SimpleQueryExecutor NewColStoreEngParallel ./Sample_Input_Files/nobench_data.json ./Sample_Input_Files/workload_50select_50insert 1 FineReadWriteLock
./run SimpleQueryExecutor NewColStoreEngParallel ./Sample_Input_Files/nobench_data.json ./Sample_Input_Files/workload_80select_20insert 1 CoarseReadWriteLock
./run SimpleQueryExecutor NewColStoreEngParallel ./Sample_Input_Files/nobench_data.json ./Sample_Input_Files/workload_80select_20insert 1 FineReadWriteLock
```

### Notice to the reviewers
Even though the git commit logs might appear that only a single user has
commited all the work, this repository is just a mirror of the internal
repository we used throughout the entire semester working on the project. All
the members of the group have worked equally on this project.

### Presentation Slides
A pdf of our presentation slides can be found
[here](https://raw.githubusercontent.com/thescouser89/faster_cheetah/master/Making_Cheetah_Faster_Presentation.pdf "Cheetah Presentation").

