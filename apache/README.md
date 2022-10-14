# Apache Arrow unpackers for Phase-2 L1 Data Scouting formats

All the tools here read data in `Native64` format and unpack it [Apache Arrow](https://arrow.apache.org/docs/cpp/index.html) native IPC format. A sample input data file can be found in the `root/data` directory upstream.

The unpackers produce a RecordBatch where the Puppi objects are saved as a List of Struct with the data contents saved as `float` or `float16`. The batch size defaults to 1 orbit.

A simple validation can be run with `make run_test_unpack`, and some performance studies with `DATAFILE=/path/to/file make run_test_speed`