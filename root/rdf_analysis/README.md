# Example analysis programs using RDataFrame

Here are some analysis programs that run using [ROOT RDataFrame](https://root.cern.ch/doc/master/classROOT_1_1RDataFrame.html)

Recipe to compile and run them
```bash
## Get the environment from the latest LCG nightly build
eval $(make env)
## Compile the code
make clean 
make all
## Run basic tests to check that the analyzers can read the different input formats
make run_tests
```

To make some speed benchmarks
```bash
DATAFILE=/path/to/file.dump make run_test_speed
```