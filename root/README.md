# ROOT unpackers for Phase-2 L1 Data Scouting formats

All the tools here read data in **Native64** format and unpack it to [ROOT](https://root.cern.ch) format, either as [TTree](https://root.cern.ch/doc/master/classTTree.html) or [RNTuple](https://root.cern/doc/v626/structROOT_1_1Experimental_1_1RNTuple.html).

For testing a small data file  SingleNeutrino.dump in **Native64** format is available under `data`.

The unpackers can also read round-robin from N files simultaneously, e.g to reassemble the raw outputs from N time-multiplexed nodes.

## Unpackers
### ROOT TTree unpacker (treeUnpacker.exe):
The new unpacker produces one list of Puppi objects, in different possible formats.
 * `float`: data members of the puppi candidates are converted to floating point values in natural units (GeV, cm), and particle IDs are converted to pdgId codes and saved as short integers. Data members available only for charged or only for neutral particles are filled with zeros for the particles of the wrong type.
 * `float24`: as above, but with truncated mantissa (ROOT calls it `Float16_t`, but it's actually 24 bits)
 * `int`: data members of the puppi candidates are unpacked to signed or unsigned integers large enough to fit the range (mainly `int16_t` or `uint16_t`) with the units natively by the L1T (e.g. 1 unit of p<sub>T</sub> is 0.25 GeV, 1 unit of &eta; or &phi; is &pi;/720), and particle IDs are saved as-is (range 0-7). Data members available only for charged or only for neutral particles are filled with zeros for the particles of the wrong type.
 * `raw64`: each puppi candidate is saved as a packed `uint64_t` word

### ROOT RNTuple unpacker (rntupleUnpacker.exe)
The unpacker have different layouts and formats:
 * `floats`: data members are unpacked to floats (same as the `float` format of the TTree unpacker above), and each variable is saved independently as `std::vector<float>` (or of short integers)
 * `coll_float`: Puppi objects are saved as a RNTuple collection, with individual variables saved as floats.
 * `raw64`:  each puppi candidate is saved as a packed `uint64_t` word

### Realtime unpacker (liveUnpacker.exe)
When used as 
```bash
./liveUnpacker.exe <kind> <format> /path/to/input /path/to/outputs 
```
This tool monitors an input directory for new files, unpacks them to the output directory and deletes the input.
 * `<kind>` can be `ttree` or `rntuple`, and `<format>` can be any of the formats supported by the TTree or RNTuple unpacker
 * to avoid race conditions, the unpacker starts when a new file is either *moved to* the input directory, or *closed after being open for writing*, and will only process files that end with `.dump`.

### Old ROOT TTree unpacker (unpack.exe, deprecated):
The old unpacker supports the same formats of then one but with one further configuration option: for everything other than raw64 the Puppi candidates can be written in `combined` format (neutrals and charged together) or `separate` (two separate `PuppiCharged` and `PuppiNeutral` sets of variables)

### Old RNTuple unpacker (rntuple_unpack.exe, deprecated)
The RNTuple output is in any of these 3 options:
 * `combined float`: using `std::vector` for all individual columns of the Puppi object variables (as the `floats` of the new unpacker)
 * `combined_coll float`: using `RNTupleModel::MakeCollection` with floats, as `coll_floats` of the new unpacker
 * `combined_struct float`: defining a simple c++ struct for the Puppi object, and using `std::vector<puppi>`.
 * `combined raw64`: as `raw64` of th new unpacker

## Running the code

The code can be compiled with `make all`.

Different tests and benchmarks are implemented, and can be run with `make <test>`:
 * `run_tests`: just tests all the different unpacker formats
 * `run_test_speed`: test speed and output size for uncompressed formats. 
 * `run_test_comp`: test speed and output size for compressed formats..
 * `run_test_multi`: runs the unpacker reading round-robin from 6 copies of the input file
 * `run_test_old`: runs the old unpackers
For most tests, if you have a larger input data sample, you can point the code to it with `DATAFILE=/path/to/file make <test>`

All this has been tested with ROOT 6.28 from the [LCG_104 build](https://lcginfo.cern.ch/release/LCG_104/) or the master in [LCG dev3](https://lcginfo.cern.ch/release/dev3/) or [LCG dev3cuda](https://lcginfo.cern.ch/release/dev3cuda/), available on CERN CVMFS via

```
source /cvmfs/sft.cern.ch/lcg/views/LCG_104/x86_64-centos8-gcc11-opt/setup.sh           # 104 on EL8 
source /cvmfs/sft.cern.ch/lcg/views/dev3cuda/latest/x86_64-centos8-gcc11-opt/setup.sh   # master on EL8
source /cvmfs/sft.cern.ch/lcg/views/dev3/latest/x86_64-el9-gcc13-opt/setup.sh           # master on EL9  
``` 

