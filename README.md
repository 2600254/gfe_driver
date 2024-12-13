---
GFE Driver
---

The GFE (Graph Framework Evaluation) Driver is the program used to run the experiments in "BACH: Bridging Adjacency List and CSR Format using LSM-Trees
for HGTAP Workloads", measuring the throughput of updates in libraries supporting structural dynamic graphs and the completion times of 
the [Graphalytics kernels](https://github.com/ldbc/ldbc_graphalytics). 
The driver supports the following systems: [Sortledton](https://gitlab.db.in.tum.de/per.fuchs/sortledton), [Teseo](https://github.com/cwida/teseo), 
[LLAMA](https://github.com/goatdb/llama), [GraphOne](https://github.com/the-data-lab/GraphOne), 
[Stinger](http://stingergraph.com/), [LiveGraph](https://github.com/thu-pacman/LiveGraph-Binary), [BACH](https://github.com/2600254/BACH) and [RocksDB](https://github.com/facebook/rocksdb). 
Additionally, it supports running the microbenchmarks from the Sortledton paper: [Microbenchmarks](https://gitlab.db.in.tum.de/per.fuchs/graph-data-structure-microbenchmarks).
It can run three kinds experiments: insert all edges in a random permuted order from an input graph, 
execute the updates specified by a [graphlog file](https://github.com/whatsthecraic/graphlog) and run the kernels of
the Graphalytics suite: BFS, PageRank (PR), local triangle counting (LCC), weighted shortest paths (SSSP), 
weakly connected components (WCC) and community detection through label propagation (CDLP).  

<!--remove third-party/libcommon/src/CMakeLists.txt:33 "-Werror"-->

### Build 

#### Requisites 
- O.S. Linux
- Autotools, [Autoconf 2.69+](https://www.gnu.org/software/autoconf/)
- A C++17 compliant compiler with support for OpenMP. We tested it with GCC 10.
- libnuma 2.0 +
- [libpapi 5.5 +](http://icl.utk.edu/papi/)
- [SQLite 3.27 +](https://sqlite.org)
- Intel Threading Building Blocks 2 (version 2020.1-2)
- Disable NUMA balancing feature to avoid the Linux Kernel to swap pages during insertions: `echo 0 | sudo tee  /proc/sys/kernel/numa_balancing`

#### Configure

Initialise the sources and the configure script by:

```
git clone https://github.com/PerFuchs/gfe_driver
cd gfe_driver
git submodule update --init
mkdir build && cd build
autoreconf -iv ..
```

The driver needs to be linked with the system to evaluate, which has to be built ahead. 
We do not recommend linking the driver with multiple systems at once, 
due to the usage of global variables in some systems and other naming clashes. 
Instead, it is safer to reconfigure and rebuild the driver each time for a single specific system.


##### BACH

Download the library from https://github.com/2600254/BACH_demo. 

```
mkdir build && cd build
cmake ..
make
```

Then configure the driver by pointing the path to the compiled dynamic library is located:

```
mkdir build && cd build
../configure --enable-optimize --disable-debug --with-bach=/path/to/BACH_demo/build
```

##### RocksDB

Download the library from https://github.com/2600254/BACH_demo. 
```
apt-get install librocksdb-dev
git clone https://github.com/facebook/rocksdb
```
Then configure the driver by pointing the path to the header files of rocksdb:

```
mkdir build && cd build
../configure --enable-optimize --disable-debug --with-rocksdb=/path/to/rocksdb
```


##### Stinger
Use the branch `feature/gfe `, it contains additional patches w.r.t. 
[upstream](https://github.com/stingergraph/stinger), from https://github.com/whatsthecraic/stinger.
For the paper, we evaluted commit "2bcfac38785081c7140b0cd27f3aecace088d664"

```
git clone https://github.com/whatsthecraic/stinger -b feature/gfe
cd stinger
mkdir build && cd stinger
cmake ../ -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=0 
make
```
If the build has been successful, it should at least create the executable `bin/stinger_server`.

Configure the GFE driver with:

```
mkdir build && cd build
../configure --enable-optimize --disable-debug --with-stinger=/path/to/stinger/build
```

##### LLAMA

Use the branch `feature/gfe `, it contains additional patches w.r.t. 
[upstream](https://github.com/goatdb/llama), from https://github.com/whatsthecraic/llama.
For the paper, we evaluated commit `32053f4ff800bed1989da79e189feeaa1bbb84b3`.

```
git clone https://github.com/whatsthecraic/llama -b feature/gfe
```

LLAMA is a header-only library. It does not need to be compiled in advance.

Configure the GFE driver with:

```
mkdir build && cd build
../configure --enable-optimize --disable-debug --with-llama=/path/to/llama
```


##### GraphOne

Use the branch `feature/gfe `, it contains additional patches w.r.t.
[upstream](https://github.com/the-data-lab/GraphOne), from https://github.com/whatsthecraic/GraphOne.
For the paper, we evaluated "1475bf5887aaf37dd7aa47377e9f11a94aa0d880".

```
git clone https://github.com/whatsthecraic/GraphOne -b feature/gfe
cd GraphOne
mkdir build && cd build
cmake -S ../ -DCMAKE_BUILD_TYPE=Release
make -j
```
If the build has been successful, it should at least create the executable `graphone64`.
Then, configure the driver with:

```
mkdir build && cd build
../configure --enable-optimize --disable-debug --with-graphone=/path/to/graphone/build
```

##### LiveGraph

Download the binary library from the [official repository](https://github.com/thu-pacman/LiveGraph-Binary/releases). 
In the paper, we evaluated version 20200829.
Then configure the driver by pointing the path to where the library has been downloading:

```
mkdir build && cd build
../configure --enable-optimize --disable-debug --with-livegraph=/path/to/livegraph/lib
```

##### Teseo

Use the branch `master` from https://github.com/cwida/teseo.
In the paper, we evaluated version `14227577731d6369b5366613f3e4a679b1fd7694`.

```
git clone https://github.com/cwida/teseo
cd teseo
./autoreconf -iv
mkdir build && cd build
../configure --enable-optimize --disable-debug
make -j
```

If the build has been successful, it should at least create the archive `libteseo.a`.
Then configure the driver with:

```
mkdir build && cd build
../configure --enable-optimize --disable-debug --with-teseo=/path/to/teseo/build   
```

##### Sortledton
Use the branch `master` from `https://gitlab.db.in.tum.de/per.fuchs/sortledton`.
For the paper, we evaluated commit "a32b8ac208bb889b518e14b1317957c9a8c466b6".

Follow the instructions in the README of the repository to setup and build the library.
Then configure the driver with:

```
mkdir build && cd build
../configure --enable-optimize --disable-debug --with-sortledton=/path/to/microbenchmark/build   
```

#### Microbenchmarks

Use the branch `master` from `https://gitlab.db.in.tum.de/per.fuchs/graph-data-structure-microbenchmarks`.
For the paper, we evaluated commit "a32b8ac208bb889b518e14b1317957c9a8c466b6".

Follow the instructions in the README of the repository to setup and build the library.
Then configure the driver with:

```
mkdir build && cd build
../configure --enable-optimize --disable-debug --with-microbenchmarks=/path/to/microbenchmark/build   
```

#### Compile

Once configured, run `make -j`. There is no `install` target, the final artifact is the executable `gfe_driver`. 

If in the mood of running the testsuite, type `make check -j`.

### Datasets

In our experiments, we used the following input graphs and data sets:

- `dota-league` and `graph500-SF`, with `SF` in {22, 24 26} and `com-friendster`, were taken from the [official Graphalytics collection](https://www.graphalytics.org/datasets).
- `uniform-SF`, with `SF` in {22, 24, 26} were generated with an [ad-hoc tool](https://github.com/whatsthecraic/uniform_graph_generator). These are synthetic graphs having the same number of vertices and edges of `graph500-SF`, but a uniform node degree distribution.
- The logs for the experiments with updates, i.e. with both insertions and deletions,
  were generated with another [ad-hoc tool](https://github.com/whatsthecraic/graphlog). 
- `yahoo-songs` and `edit-enwiki` were taken from the [Konect webpage](http://konect.cc/networks/) they were prepared 
  for our experiments by sorting them by timestamp and removing duplicates by using `tools/timestampd_graph_2_edge_list.py`.  
- `uk-2007-05` can be found at [there](https://1drv.ms/u/c/db6587cfda88e436/EU-ZDzgc321Elbu8RJsbK4ABgiFabEzsdTxF8F87kFJ65w?e=UbF0AP), which is derived by reformatting the data from [there](https://sites.cc.gatech.edu/dimacs10/archive/clustering.shtml).

A complete image of all datasets used in the experiments can be downloaded from Zenodo: [input graphs](https://zenodo.org/record/3966439),
[graph logs](https://zenodo.org/record/3967002), [dense friendster](https://zenodo.org/record/5146230) and [timestamped graphs](https://zenodo.org/record/5752476).

### Executing the driver


The driver takes as input a list of options together with a graph, and emits the results into a sqlite3 database.
There are three kinds of experiments that can be executed:

- **Insertions only**: insert all vertices and edges from an input graph, in a random order. Use the command:

```
./gfe_driver -G /path/to/input/graph.properties -u -l <system_to_evaluate> -w <num_threads> -d output_results.sqlite3
```

For LLAMA only: add the option `--build_frequency 10s` to asynchronously issue the creation of a new level (or delta) every 10 seconds.

- **Updates**: perform all insertions and deletions from a log. Add the option --log /path/to/updates.graphlog :

```
./gfe_driver -G /path/to/input/graph.properties -u --log /path/to/updates.graphlog --aging_timeout 24h -l <system_to_evaluate> -w <num_threads> -d output_results.sqlite3
```

- **Graphalytics**: execute the six kernels from the Graphalytics suite. Add the option `-R <N>` to repeat `N` times the execution of all Graphalytics kernels, one after the other. E.g., to run the kernels five times, after all vertices and edges have been inserted, use:

```
./gfe_driver -G /path/to/input/graph.properties -u -l <system_to_evaluate> -w <num_threads> -R 5 -d output_results.sqlite3
```

Type `./gfe_driver -h` for the full list of options and for the libraries that can be evaluated (option `-l`). The driver spawns the number of threads given by the option `-w` to concurrently run all insertions or updates. For Graphalytics, it defaults to the total number of the physical threads in the machine. This setting can be changed with the option `-r <num_threads>`. Note that the numbers
in the library codes (e.g. teseo.**6**, stinger**3**) are unrelated to the versions of the systems evaluated, they were only used
internally for development purposes.

The database `output_results.sqlite3` will contain the final results. Refer to [this repository](https://github.com/whatsthecraic/gfe_notebooks) to see how to load and inspect the data within Jupyter notebooks and how to recreate the same plots of the paper.

### Repeating the experiments

These are the full commands to repeat the experiments in the paper:

##### GTP Throughput (Figure 6)

```bash
libraries=("bach_ro_leveling" "bach_ro_tiering" "livegraph3_ro" "rocksdb" "llama8-ref")
datasets=("datagen-8_5-fb" "graph500-26" "com-friendster" "uk-2007-05")
for dataset in "${datasets[@]}"; do
  for library in "${libraries[@]}"; do
    ./gfe_driver -u -d results.sqlite3 -l $library -G /path/to/input/$dataset.properties -w 16 --latency true 
    ./gfe_driver -u -d results.sqlite3 -l $library -G /path/to/input/$dataset.properties -w 16 --latency true --is_timestamped true
    ./gfe_driver -u -d results.sqlite3 -l $library -G /path/to/input/$dataset.properties -w 16 --log /path/to/input/$dataset-0.3.graphlog --aging_timeout 10h --latency true --is_timestamped true
  done
done
```

##### Graph Analytical Processing Performance (Figure 7(A)-(F))

```bash
libraries=("bach_ro_leveling" "bach_ro_tiering" "livegraph3_ro" "rocksdb" "llama8-ref")
datasets=("datagen-8_5-fb" "graph500-26" "com-friendster" "uk-2007-05")
for dataset in "${datasets[@]}"; do
  for library in "${libraries[@]}"; do
    ./gfe_driver -u -R 5 -d results.sqlite3 -l $library -G /path/to/input/$dataset.properties -w 16  
  done
done
```

##### HGTAP Performance (Figure 8(a))
For all graphs with 5 runs.

```bash
libraries_bach=("bach_ro_elastic" "bach_ro_tiering" "bach_ro_leveling")
datasets=("datagen-8_5-fb" "graph500-26" "com-friendster")
for dataset in "${datasets[@]}"; do
  for library in "${libraries_bach[@]}"; do
    ./gfe_driver -u -d results.sqlite3 -l $library -G /path/to/input/$dataset.properties -w 16 -r 16 --log /path/to/input/$dataset-0.5.graphlog --aging_timeout 10h --mixed_workload true -R 1
  done
done
libraries=("livegraph3_ro" "rocksdb")
for dataset in "${datasets[@]}"; do
  for library in "${libraries[@]}"; do
    ./gfe_driver -u -d results.sqlite3 -l $library -G /path/to/input/$dataset.properties -w 16 -r 16 --log /path/to/input/$dataset-0.5.graphlog --aging_timeout 10h --mixed_workload true -R 1
  done
done
for dataset in "${datasets[@]}"; do
  for library in "${libraries_bach[@]}"; do
    ./gfe_driver -u -d results.sqlite3 -l $library -G /path/to/input/graph500-26.properties -w 16 -r 16 --log /path/to/input/graph500-26-0.5-sq.graphlog --aging_timeout 10h --mixed_workload true -R 1 --is_timestamped true
  done
done
for dataset in "${datasets[@]}"; do
  for library in "${libraries[@]}"; do
    ./gfe_driver -u -d results.sqlite3 -l $library -G /path/to/input/graph500-26.properties -w 16 -r 16 --log /path/to/input/graph500-26-0.5-sq.graphlog --aging_timeout 10h --mixed_workload true -R 1 --is_timestamped true
  done
done
```

The option `--aging_timeout` serves to limit the total time to execute the experiment.
For LLAMA, it could be necessary to stop the experiment earlier, as the continuous creation of new deltas
can cause a memory exhaustion.  
For the experiment with the memory footprint of Figure 7d, add the arguments: 
`--aging_memfp --aging_memfp_physical --aging_memfp_threshold 330G --aging_release_memory=false`.
The option `--aging_memfp` records the memory footprint as the experiment proceeds,
`--aging_memfp_physical` records the physical memory (RSS) of the process, rather than the virtual memory
of the glibc allocator, `--aging_memfp_threshold 330G` terminates the experiment
if the memory footprint measured is greater than 330 GB and 
`--aging_release_memory=false` avoids releasing the memory used in the driver to load the graph 
from the file, as it may (or may not) recycled by the libraries. 
With the memory footprint, for LLAMA, it's not necessary to set `--aging_timeout 4h` as 
`--aging_memfp_threshold 330G` already acts as a guard on the overall memory consumption.
