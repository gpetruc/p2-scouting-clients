#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/pretty_print.h>
#include <arrow/status.h>
#include <arrow/result.h>
#include <arrow/table.h>

#include <iostream>
#include <cstdio>
#include <chrono>
#include <string>

#include <getopt.h>

using arrow::Status;

void usage() {
  printf("Usage: arrow_read.exe [options] <format> infile.arrow \n");
  printf("   layout := ipc \n");
  printf("Options:\n");
  printf("  -j, --cputhreads N: set the capacity of the global CPU thread pool\n");
  printf("  -J, --iothreads N: set the capacity of the global I/O thread pool\n");
}

Status run_ipc(const char *fname) {
  std::cout << "Reading " << fname << std::endl;
  ARROW_ASSIGN_OR_RAISE(auto file, arrow::io::ReadableFile::Open(fname));
  std::cout << "Opened file " << fname << std::endl;
  ARROW_ASSIGN_OR_RAISE(auto reader, arrow::ipc::RecordBatchStreamReader::Open(file));
  std::cout << "Created reader " << std::endl;
  ARROW_ASSIGN_OR_RAISE(auto table, arrow::Table::FromRecordBatchReader(&*reader));
  std::cout << "Read table " << std::endl;
  ARROW_RETURN_NOT_OK(arrow::PrettyPrint(*table, {}, &std::cout));
  return Status::OK();
}
int main(int argc, char **argv) {
  if (argc < 3) {
    usage();
    return 1;
  }
  unsigned int iothreads = -1, cputhreads = -1;
  while (1) {
    static struct option long_options[] = {{"help", no_argument, nullptr, 'h'},
                                           {"iothreads", required_argument, nullptr, 'J'},
                                           {"cputhreads", required_argument, nullptr, 'j'},
                                           {nullptr, 0, nullptr, 0}};
    int option_index = 0;
    int optc = getopt_long(argc, argv, "hj:J:b:", long_options, &option_index);
    if (optc == -1)
      break;

    switch (optc) {
      case 'h':
        usage();
        return 0;
      case 'J':
        iothreads = std::atoi(optarg);
        arrow::io::SetIOThreadPoolCapacity(iothreads);
        break;
      case 'j':
        cputhreads = std::atoi(optarg);
        arrow::SetCpuThreadPoolCapacity(cputhreads);
        break;
      default:
        usage();
        return 1;
    }
  }
  int iarg = optind, narg = argc - optind;
  std::string format = argv[iarg];
  std::chrono::time_point<std::chrono::steady_clock> tstart, tend;
  tstart = std::chrono::steady_clock::now();
  if (format == "ipc") {
    Status st = run_ipc(argv[iarg + 1]);
    if (!st.ok()) {
      std::cout << st << std::endl;
      return 1;
    }
  }
  tend = std::chrono::steady_clock::now();
  double time = (std::chrono::duration<double>(tend - tstart)).count();
  return 0;
}