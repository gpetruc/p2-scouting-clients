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

This directory contains a single C++ client `data_checker` that can receive data via TCP/IP or read it from files, and check different formats.

 ```cpp
make && make run_tests
 ```
