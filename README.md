# Client code for Phase-2 L1 Data Scouting

This repository contains software clients to receive and process Phase-2 L1 Data Scouting data.
 * Main directory: C++ clients to receive scouting data via TCP/IP
 * `root` directory: clients to unpack data to [ROOT](https://root.cern.ch) [TTree](https://root.cern.ch/doc/master/classTTree.html) or [RNTuple](https://root.cern/doc/v626/structROOT_1_1Experimental_1_1RNTuple.html) formats.
 * `apache` directory: clients to unpack data to [Apache Arrow](https://arrow.apache.org/docs/cpp/index.html) format

 ## Raw event formats

 * **Native64**: each event has a 64 bit event header followed by N Puppi candidates (64 bits each). The event header format is

   | bits  | size | meaning |
   |-------|------|---------|
   | 63-62 |   2  |  `10` = valid event header |
   | 61    |   1  | error bit |
   | 60-56 |   6  | (local) run number |
   | 55-24 |  32  | orbit number |
   | 23-12 |  12  | bunch crossing number (0-3563) |
   | 11-07 |   4  | must be set to `0` |
   | 07-00 |   8  | number of Puppi candidates |

      * An event is identified  _truncated_ if it's header reports a length of 0 candidates, and the error bit is set.

* **Native128**: each event has a 64 bit event header followed by N Puppi candidates (64 bits each), and is padded to a multiple of 128 bits. 
  * An event is identified  _truncated_ if it's header reports a length of 0 candidates, but the following padding word is not zero.

* **DTHBasic**: each event has a 128 bit [DTH v1p2 header](https://gitlab.cern.ch/dth_p1-v2/dth_2srs_2srr_1daq/-/blob/master/top.srcs/sources_1/new/frag_to_blocks/Memory_blocks.vhd), a 128 bit [SlinkRocket](https://edms.cern.ch/file/2502737/2/cms_phase2_slinkrocket.pdf) header, the event padded to 128 bits (as **Native128** above), and a SlinkRocket trailer.

* **DTHBasicOA**: each orbit is split in one or more DTH v1p2 data blocks. All blocks except the last have fixed size of 4 kB. Each block starts with a 128 bit [DTH v1p2 header](https://gitlab.cern.ch/dth_p1-v2/dth_2srs_2srr_1daq/-/blob/master/top.srcs/sources_1/new/frag_to_blocks/Memory_blocks.vhd). The orbit data has a 128 bit [SlinkRocket](https://edms.cern.ch/file/2502737/2/cms_phase2_slinkrocket.pdf) header, the events in **Native128** format, and a SlinkRocket trailer. Note that events can be broken across two DTH data blocks. 
  * A _truncated_ orbit has only the SlinkRocket header and trailer, and no events inside.

Two small example data files in **DTHBasic** and **DTHBasicOA** formats are available in the `data` directory.

* **DTHBasic256**: each orbit is split in one or more DTH v1p2-256 data blocks. All blocks except the last have fixed size of 8 kB. Each block starts with a 256 bit DTH header (the format is the same as the 128 bit header above plus another 128 bits of zeros, but the size is in units of 256 bits instead of 128 bits). The DTH is followed by events in the **Native64** format as above, and there may be a padding of zeros to make the orbit be a multiple of 256 bits. Note that events can be broken across two DTH data blocks. 
      * A _truncated_ orbit has only one Orbit header with the error bit set to 1 and size set to zero, i.e. 
  
   | bits  | size | meaning |
   |-------|------|---------|
   | 63-62 |   2  |  `11` = valid orbit header |
   | 61    |   1  | error bit |
   | 60-56 |   6  | (local) run number |
   | 55-24 |  32  | orbit number |
   | 23-00 |  24  | size in 64 bit words |

 ## Main

This directory contains a single C++ client `data_checker` that can receive data via TCP/IP or read it from files, and check different formats, and a dummy data generator `data_generator` that can output data.

 ```cpp
make && make run_tests
 ```
### Data checker & receiver

```
./data_checker.exe [options] Mode Source [arguments]
```
*Source* can be a file, including a named fifo, or a TCP/IP "address:port". If a `%d` is found in the file name, it's replaced by the index of the client (starting from zero).
*Modes* that receive the data and perform some validity checks on it are:
 * `Native64`, `Native128`: files in the formats described above
 * `Native64SZ`: file in `Native64` format but allowing possible null 64 bit words between events
 * `DTHBasic`, `DTHBasicOA`: as described above, but note that the non-OA is not really supported in recent DTH firmwares
 * `DTHBasicOA-NC`: read `DTHBasicOA` format but only validates the DTH header, not the events inside
 * `DTHBasicOA-NoSR`: same format as `DTHBasicOA` but without SlinkRocket headers and trailers 
 * `DTHBasic256`, `DTHBasic256-NC`: read DTHBasic256 format. The `-NC` version only checks DTH headers and not the contents of the orbit payload itself.
 * `DTHBasic256`, `DTHBasic256-NC`: read DTHBasic256 format. The `-NC` version only checks DTH headers and not the contents of the orbit payload itself.
*Modes* that receive and store some data are:
 * `DTHReceiveOA`: when used as `data_checker.exe DTHReceiveOA source output [prescale]` it will receive in `DTHBasicOA` format and saves a prescaled amount of orbits out in `Native128` format. The default prescale is 100. It stops after collecting 4GB of data.
 * `DTHReceive256`: when used as `data_checker.exe DTHReceive256 source output [prescale]` it will receive in `DTHBasic256` format and saves a prescaled amount of orbits out in `Native64` format with up to 3 null 64-bit words at the end of each orbit (so, it can be read back with `Native64SZ`). The default prescale is 100. It stops after collecting 4GB of data.
 * `DTHRollingReceive256`: when used as `data_checker.exe DTHRollingReceive256 source outputBasePath orbitsPerFile` it will receive in `DTHBasic256` format and saves data out in `Native64` format with up to 3 null 64-bit words at the end of each orbit (so, it can be read back with `Native64SZ`), with a specified number of orbits per file. Outputs are saved as outputBasePath + `tsNN.orbNNNNNNNN.dump` where NN is the timeslice index (argument of option `-t`) and `NNNNNNNN` is the orbit number of the first orbit in the file.
*Modes* for debugging are
 * `TrashData`: receive data via TCP/IP and discard it without any processing
 * `ReceiveAndStore`: when used as `data_checker.exe ReceiveAndStore source sizeInGB outputFile`: receive data up to sizeInGB data and save it in outputFile without any processing.
Useful *options* are:
 * `-h`: lists all the options
 * `-n`: receives `n` streams instead of 1. For TCP/IP, the streams receive on consecutive ports, so receiving e.g. on port 7777 with `-n 2` will open a server on 7777 and one on 7778. For files, a `%d` has to be specified in the file name. 
  * Normally, the timeslice index is incremented for each stream, so stream 0 expects timeslice 0, stream 1 expects timeslice 1, etc (this is not done if `--orbitmux N` is specified with N > 1)
 * `-k`: restart the servers after the connection is closed (use `<ctrl>+c` to terminate the job)
 * `--maxorbits N`: stops after processing N orbits)

### Event generator

The even generator reads some events in `Native64` format, and resamples them in order to generate a stream of data.
```
./data_generator.exe [options] Mode root/data/SingleNeutrino.dump Destination
```

Supported *modes* at the moment are just  `Native64` and `DTHBasic256`

*Destinations* can be files or fifos (possibly with a `%d`` in the name, when generating multiple streams), or `ip:port` for TCP connections.

Useful *options* are:
 * `-n N`: generate N streams of data
 * `--orbits N` specifies how many orbits to generate

#### Example of data generation and receiving with 2 streams, in DTH256 format

In a first shell, do
```bash
make all
mkfifo /run/user/$UID/tmpfifo.0
mkfifo /run/user/$UID/tmpfifo.1
./data_checker.exe DTHRollingReceive256 /run/user/$UID/tmpfifo.%d /run/user/$UID/raw 1000 -n 2
```
in a second shell do
```bash
./data_generator.exe DTHBasic256 root/data/SingleNeutrino.dump  /run/user/$UID/tmpfifo.%d --orbits 10000 -n 2
```

TCP/IP can also be used, but FIFOs are faster.

