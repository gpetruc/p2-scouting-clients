#include <TROOT.h>
#include <TFile.h>
#include <ROOT/RLogger.hxx>
#include <ROOT/RNTuple.hxx>
#include "RootUnpackMaker.h"
#include "../unpack.h"
#include <getopt.h>

void usage() {
  printf("Usage: rootUnpacker.exe [ options ] <obj> <kind> <format> infile.dump [infile2.dump ...] [ outfile.root ]\n");
  printf("  obj  := puppi | tkmu \n");
  printf("  kind := ttree | rntuple\n");
  printf("  puppi ttree  formats   := float | float24 | int | raw64\n");
  printf("  puppi rntule formats   := floats | coll_float | ints | coll_int | raw64\n");
  printf("  tkmu  ttree  formats   := float | float24 | int\n");
  printf("  tkmu  rntule formats   := floats | coll_float\n");
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

  int ret = 0;
  try {
    std::unique_ptr<UnpackerBase> unpacker =
        RootUnpackMaker::make(obj, kind, format, compressionMethod, compressionLevel);
    if (!unpacker) {
      printf("Unsupported object type %s, file type %s and/or output format %s\n",
             obj.c_str(),
             kind.c_str(),
             format.c_str());
      return 1;
    }
    ROOT::EnableThreadSafety();
    if (threads != -1)
      unpacker->setThreads(threads);
    auto report = unpacker->unpackFiles(ins, output);
    printReport(report);
  } catch (const std::exception &e) {
    printf("Terminating an exception was raised:\n%s\n", e.what());
    ret = 1;
  }
  return ret;
}
