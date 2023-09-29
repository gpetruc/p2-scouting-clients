# ROOT unpackers for Phase-2 L1 Data Scouting formats

All the tools here read data in **Native64** format and unpack it to [ROOT](https://root.cern.ch) format, either as [TTree](https://root.cern.ch/doc/master/classTTree.html) or [RNTuple](https://root.cern/doc/v626/structROOT_1_1Experimental_1_1RNTuple.html).

For testing a small data file Puppi.dump in **Native64** format is available under `data`.

The unpackers can also read round-robin from N files simultaneously, e.g to reassemble the raw outputs from N time-multiplexed nodes.

## Unpackers
### ROOT file unpacker (rootUnpacker.exe):

The unpacker can be run as `rootUnpacker.exe <objType> <fileType> <format> inputs.dump [output.root]`.
 * The object type can be `puppi` for Puppi candidates, and `tkmu` for GMT TkMuons
 * The file type can be `ttree` or `rntuple`
 * The each object type and file type supports different fromats, as documented below

#### Puppi object TTree formats (`rootUnpacker.exe puppi ttree`)
The unpacker produces one list of Puppi objects, in different possible formats.
 * `float`: data members of the puppi candidates are converted to floating point values in natural units (GeV, cm), and particle IDs are converted to pdgId codes and saved as short integers. Data members available only for charged or only for neutral particles are filled with zeros for the particles of the wrong type.
 * `float24`: as above, but with truncated mantissa (ROOT calls it `Float16_t`, but it's actually 24 bits)
 * `int`: data members of the puppi candidates are unpacked to signed or unsigned integers large enough to fit the range (mainly `int16_t` or `uint16_t`) with the units natively by the L1T (e.g. 1 unit of p<sub>T</sub> is 0.25 GeV, 1 unit of &eta; or &phi; is &pi;/720), and particle IDs are saved as-is (range 0-7). Data members available only for charged or only for neutral particles are filled with zeros for the particles of the wrong type.
 * `raw64`: each puppi candidate is saved as a packed `uint64_t` word

#### Puppi object RNTuple formats (`rootUnpacker.exe puppi rntuple`)
The unpacker have different layouts and formats:
 * `floats`: data members are unpacked to floats (same as the `float` format of the TTree unpacker above), and each variable is saved independently as `std::vector<float>` (or of short integers)
 * `coll_float`: Puppi objects are saved as a RNTuple collection, with individual variables saved as floats.
 * `ints`, `coll_int`: data members are unpacked to integers, as per the `int` format of the TTree unpacker, and saved as std::vector's (`ints`) or using a RNTuple collection (`coll_int`).
 * `raw64`:  each puppi candidate is saved as a packed `uint64_t` word

#### TkMu object TTree formats (`rootUnpacker.exe tkmu ttree`)
The unpacker produces one list of TkMu objects, in different possible formats.
 * `float`: data members of the TkMu candidates are converted to floating point values in natural units (GeV, cm), and the charge is converted to an int8 (values &plusmn;1). The isolation bitmask and quality values are kept as uint8.
 * `float24`: as above, but with truncated mantissa (ROOT calls it `Float16_t`, but it's actually 24 bits)
 * `int`: data members of the TkMu candidates are unpacked to signed or unsigned integers large enough to fit the range (mainly `int16_t` or `uint16_t`) with the units natively by the L1T (e.g. 1 unit of p<sub>T</sub> is 31.25 MeV, 1 unit of &eta; or &phi; is &pi;/2<sup>12</sup>). The charge is still converted to an int8 (values &plusmn;1).

#### TkMu object RNTuple formats (`rootUnpacker.exe tkmu rntuple`)

The unpacker supports these formats:
 * `floats`: data members are unpacked to floats (same as the `float` format of the TTree unpacker above), and each variable is saved independently as `std::vector<float>` (or of short integers)
 * `coll_float`: TkMu objects are saved as a RNTuple collection, with individual variables saved as floats.

### Realtime unpacker (liveUnpacker.exe)

In order to get the TBB library, this needs to be compiled passing the path to it (if you used the LCG environment set up with `make envdev` in the parent directory, this is already done for you):
```bash
export TBB=/path/to/tbb
make liveUnpacker.exe
```
it can then be run as 
```bash
./liveUnpacker.exe <objType> <fileKind> <format> /path/to/input /path/to/outputs 
```
This tool monitors an input directory for new files, unpacks them to the output directory and deletes the input.
 * `<objType>` can be `puppi` or `tkmu`, `<fileKind>` can be `ttree` or `rntuple`, and `<format>` can be any of the formats supported by the TTree or RNTuple unpackers for that object type.

The main options are:
 * `-j N`: run multithread using up to N cpus to unpack files in parallel as they arrive.
   * FIXME: that the multithreaded live unpacker doesn't seem to respond to _ctrl+C_, so the way to stop it is to send it to background with _ctrl+Z_ and then `kill -9 %`. 
 * `--delete`: delete also the _output_ files once the unpacking is completed (for benchmarking, so that one doesn't fill up the ramdisk if there's no consumer process)
 * `--demux T`: time-demultiplex inputs that arrive from T timeslices. Input filenames must conform to what DTHRollingReceive256 produces.

To avoid race conditions, the unpacker starts when a new file is either *moved to* the input directory, or *closed after being open for writing*, and will only process files that end with `.dump`.
The file is renamed to `.dump.taken` before starting the processing, and the output is called `.tmp.root` until it's complete.

## Running the code

The code can be compiled with `make all`.

You need to have `root` in your path, and for the live unpacker you also need the intel TBB libraries installed and have the `TBB` environment variable pointing to where they are installed. Doing `eval $(make envdev)` (EL8) or `eval $(make env9dev)` (EL9) in the parent directory of this will set up an appropriate LCG environment for compiling the code.

Different tests and benchmarks are implemented, and can be run with `make <test>`:
 * `run_tests`: just tests all the different unpacker formats
 * `run_test_speed`: test speed and output size for puppi uncompressed formats. 
 * `run_test_comp`: test speed and output size for puppi compressed formats..
 * `run_test_multi`: runs the unpacker reading round-robin from 6 copies of the puppi input file
 * `run_test_tkmu`:  test speed and output size for tkmu formats (uncompressed and compressed)
For most tests, if you have a larger input data sample, you can point the code to it with `DATAFILE=/path/to/file make <test>`

All this has been tested with ROOT 6.28 from the [LCG_104 build](https://lcginfo.cern.ch/release/LCG_104/) or the master in [LCG dev3](https://lcginfo.cern.ch/release/dev3/) or [LCG dev3cuda](https://lcginfo.cern.ch/release/dev3cuda/), available on CERN CVMFS via

```
source /cvmfs/sft.cern.ch/lcg/views/dev3cuda/latest/x86_64-centos8-gcc11-opt/setup.sh   # master on EL8
source /cvmfs/sft.cern.ch/lcg/views/LCG_104/x86_64-centos8-gcc11-opt/setup.sh           # 104 on EL8 
source /cvmfs/sft.cern.ch/lcg/views/dev3/latest/x86_64-el9-gcc13-opt/setup.sh           # master on EL9  
``` 

