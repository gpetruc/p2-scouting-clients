#include <TStopwatch.h>
#include <ROOT/RLogger.hxx>
#include "RNTupleUnpackerBase.h"
#include "RNTupleUnpackerFloats.h"
#include "RNTupleUnpackerCollFloat.h"
#include "RNTupleUnpackerInts.h"
#include "RNTupleUnpackerCollInt.h"
#include "RNTupleUnpackerRaw64.h"
#include "GMTTkMuRNTupleUnpackerFloats.h"
#include "GMTTkMuRNTupleUnpackerCollFloat.h"
#include <getopt.h>

void usage() {
  printf("Usage: rntupleUnpacker.exe [ options ] <obj> <format> infile.dump [infile2.dump ...] [ outfile.root ]\n");
  printf("  obj := puppi | tkmu \n");
  printf("  puppi format   := floats | coll_float | ints | coll_int | raw64\n");
  printf("  tkmu  format   := floats | coll_float\n");
  printf("Options: \n");
  printf("  -j N            : multithread with N threads\n");
  printf("  -z algo[,level] : enable compression\n");
  printf("                    algorithms supported are none, lzma, zlib, lz4, zstd;\n");
  printf("                    default level is 4\n");
}
int main(int argc, char **argv) {
  if (argc < 4) {
    usage();
    return 1;
  }

  auto verbosity =
      ROOT::Experimental::RLogScopedVerbosity(ROOT::Experimental::NTupleLog(), ROOT::Experimental::ELogLevel::kError);

  std::string compressionMethod = "none";
  int compressionLevel = 0, threads = -1;
  while (1) {
    static struct option long_options[] = {{"help", no_argument, nullptr, 'h'},
                                           {"threads", required_argument, nullptr, 'j'},
                                           {"compression", required_argument, nullptr, 'z'},
                                           {nullptr, 0, nullptr, 0}};
    int option_index = 0;
    int optc = getopt_long(argc, argv, "hj:z:", long_options, &option_index);
    if (optc == -1)
      break;

    switch (optc) {
      case 'h':
        usage();
        return 0;
      case 'j':
        threads = std::atoi(optarg);
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
  std::string obj = std::string(argv[iarg]);
  std::string format = std::string(argv[iarg + 1]);
  printf("Will run rntuple %s with format %s\n", argv[iarg], argv[iarg + 1]);

  std::vector<std::string> ins;
  std::string output;

  for (int i = iarg + 2; i < iarg + narg; ++i) {
    std::string fname = argv[i];
    if (fname.length() > 5 && fname.substr(fname.length() - 5) == ".root") {
      if (!output.empty()) {
        printf("Multiple output root files specified in the command line\n");
        return 2;
      }
      output = fname;
    } else {  // assume it's an input file
      ins.emplace_back(argv[i]);
    }
  }

  std::unique_ptr<RNTupleUnpackerBase> unpacker;
  int ret = 0;
  try {
    if (obj == "puppi") {
      if (format == "floats") {
        unpacker = std::make_unique<RNTupleUnpackerFloats>();
      } else if (format == "coll_float") {
        unpacker = std::make_unique<RNTupleUnpackerCollFloat>();
      } else if (format == "ints") {
        unpacker = std::make_unique<RNTupleUnpackerInts>();
      } else if (format == "coll_int") {
        unpacker = std::make_unique<RNTupleUnpackerCollInt>();
      } else if (format == "raw64") {
        unpacker = std::make_unique<RNTupleUnpackerRaw64>();
      }
    } else if (obj == "tkmu") {
      if (format == "floats") {
        unpacker = std::make_unique<GMTTkMuRNTupleUnpackerFloats>();
      } else if (format == "coll_float") {
        unpacker = std::make_unique<GMTTkMuRNTupleUnpackerCollFloat>();
      }
    }
    if (!unpacker) {
      printf("Unsupported object type %s and/or output format %s\n", obj.c_str(), format.c_str());
      return 1;
    }
    unpacker->setCompression(compressionMethod, compressionLevel);
    if (threads != -1)
      unpacker->setThreads(threads);
    auto report = unpacker->unpack(ins, output);
    printReport(report);
  } catch (const std::exception &e) {
    printf("Terminating an exception was raised:\n%s\n", e.what());
    ret = 1;
  }
  return ret;
}
