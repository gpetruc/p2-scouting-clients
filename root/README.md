# ROOT unpackers for Phase-2 L1 Data Scouting formats

All the tools here read data in **Native64** format and unpack it to [ROOT](https://root.cern.ch) format, either as [TTree](https://root.cern.ch/doc/master/classTTree.html) or [RNTuple](https://root.cern/doc/v626/structROOT_1_1Experimental_1_1RNTuple.html).

For testing a small data file  SingleNeutrino.dump in **Native64** format is available under `data`.

The TTree output can be in `combined` format (one list of Puppi objects) or `separate` format (two lists, for charged and neutral Puppi candidates), saved as either `int`s, `float`s or `float24` (which ROOT calls `Float16_t`), either uncompressed or compressed. A basic validation of the different formats can be run with `make run_test_unpack`

The RNTuple output is currently only in `float` format, in any of these 3 options:
 * `combined`: using `std::vector` for all individual columns of the Puppi object variables
 * `combined_coll`: using `RNTupleModel::MakeCollection` 
 * `combined_struct`: defining a simple c++ struct for the Puppi object, and using `std::vector<puppi>`.
 A comparison of the different formats in terms of speed and size can be run with `make run_test_speed`. If you have a larger input data sample, you can point the code to it with `DATAFILE=/path/to/file make run_test_speed`.

All this has been tested with ROOT 6.28 from the [LCG dev4cuda build](https://lcginfo.cern.ch/release/dev4cuda/), available on CERN CVMFS via
```
source /cvmfs/sft.cern.ch/lcg/views/dev4cuda/latest/x86_64-centos8-gcc11-opt/setup.sh
``` 

