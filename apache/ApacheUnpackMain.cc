
#include "ApacheUnpackMaker.h"
#include <arrow/io/api.h>
#include <arrow/util/thread_pool.h>
#include <getopt.h>
#include "../unpack.h"

void usage() {
  printf("Usage: rootUnpacker.exe [ options ] <obj> <kind> <format> infile.dump [infile2.dump ...] [ outfile.root ]\n");
  printf("  obj  := puppi | tkmu \n");
  printf("  kind := ipc\n");
  printf("  puppi ipc  formats   := float | float16 | int | raw64\n");
  printf("  tkmu  ipc  formats   := float | float16\n");
  printf("Options: \n");
  printf("  -j, --cputhreads N: set the capacity of the global CPU thread pool\n");
  printf("  -J, --iothreads  N: set the capacity of the global I/O thread pool\n");
  printf("  -z algo[,level] : enable compression\n");
  printf("                    for IPC files, algorithms supported are none, lz4, zstd (if compiled in)\n");
  printf("                    default level is 4\n");
}

bool is_output_fname(const std::string &fname) {
  if (fname.length() > 6 && fname.substr(fname.length() - 6) == ".arrow")
    return true;
  if (fname.length() > 4 && fname.substr(fname.length() - 4) == ".ipc")
    return true;
  return false;
}

int main(int argc, char **argv) {
  if (argc < 4) {
    usage();
    return 1;
  }

  std::string compressionMethod = "none";
  int compressionLevel = 0;
  unsigned long batchsize = 3564;  // default is number of BXs in one LHC orbit
  while (1) {
    static struct option long_options[] = {{"help", no_argument, nullptr, 'h'},
                                           {"batchsize", required_argument, nullptr, 'b'},
                                           {"iothreads", required_argument, nullptr, 'J'},
                                           {"cputhreads", required_argument, nullptr, 'j'},
                                           {"compression", required_argument, nullptr, 'z'},
                                           {nullptr, 0, nullptr, 0}};
    int option_index = 0;
    int optc = getopt_long(argc, argv, "hj:J:b:z:", long_options, &option_index);
    if (optc == -1)
      break;

    switch (optc) {
      case 'h':
        usage();
        return 0;
      case 'b':
        batchsize = std::atoi(optarg);
        break;
      case 'J':
        arrow::io::SetIOThreadPoolCapacity(std::atoi(optarg));
        break;
      case 'j':
        arrow::SetCpuThreadPoolCapacity(std::atoi(optarg));
        break;
      case 'z': {
        compressionMethod = std::string(optarg);
        auto pos = compressionMethod.find(",");
        if (pos != std::string::npos) {
          compressionLevel = std::atoi(compressionMethod.substr(pos + 1).c_str());
          compressionMethod = compressionMethod.substr(0, pos);
        } else {
          compressionLevel = 4;
        }
      } break;
      default:
        usage();
        return 1;
    }
  }

  int iarg = optind, narg = argc - optind;
  if (narg < 4) {
    usage();
    return 1;
  }
  std::string obj = std::string(argv[iarg]);
  std::string kind = std::string(argv[iarg + 1]);
  std::string format = std::string(argv[iarg + 2]);
  printf("Will run %s %s with format %s\n", argv[iarg], argv[iarg + 1], argv[iarg + 2]);

  std::vector<std::string> ins;
  std::string output;

  for (int i = iarg + 3; i < iarg + narg; ++i) {
    std::string fname = argv[i];
    if (is_output_fname(fname)) {
      if (!output.empty()) {
        printf("Multiple output root files specified in the command line\n");
        return 2;
      }
      output = fname;
    } else {  // assume it's an input file
      ins.emplace_back(argv[i]);
    }
  }

  int ret = 0;
  try {
    std::unique_ptr<UnpackerBase> unpacker =
        ApacheUnpackMaker::make(batchsize, obj, kind, format, compressionMethod, compressionLevel);
    if (!unpacker) {
      printf("Unsupported object type %s, file type %s and/or output format %s\n",
             obj.c_str(),
             kind.c_str(),
             format.c_str());
      return 1;
    }
    auto report = unpacker->unpackFiles(ins, output);
    printReport(report);
  } catch (const std::exception &e) {
    printf("Terminating an exception was raised:\n%s\n", e.what());
    ret = 1;
  }
  return ret;
}