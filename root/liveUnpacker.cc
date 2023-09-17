#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <errno.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/inotify.h>
#include <TStopwatch.h>
#include "TTreeUnpackerFloats.h"
#include "TTreeUnpackerInts.h"
#include "TTreeUnpackerRaw64.h"
#include "RNTupleUnpackerFloats.h"
#include "RNTupleUnpackerCollFloat.h"
#include "RNTupleUnpackerRaw64.h"
#include <getopt.h>
#include <libgen.h>

void usage() {
  printf("Usage: liveUnpacker.exe [ options ] <kind> <format> /path/to/input /path/to/outputs \n");
  printf("  kind             := ttree rntuple\n");
  printf("  ttree  formats   := float | float24 | int | raw64\n");
  printf("  rntule formats   := floats | coll_float | raw64\n");
  printf("Options: \n");
  printf("  -j N            : multithread with N threads\n");
  printf("  -z algo[,level] : enable compression\n");
  printf("                    algorithms supported are none, lzma, zlib, lz4, zstd;\n");
  printf("                    default level is 4\n");
}

std::unique_ptr<UnpackerBase> makeUnpacker(const std::string &kind,
                                           const std::string &format,
                                           const std::string &compressionMethod,
                                           int compressionLevel) {
  std::unique_ptr<UnpackerBase> unpacker;
  if (kind == "ttree") {
    if (format == "float" || format == "float24") {
      unpacker = std::make_unique<TTreeUnpackerFloats>(format);
    } else if (format == "int") {
      unpacker = std::make_unique<TTreeUnpackerInts>();
    } else if (format == "raw64") {
      unpacker = std::make_unique<TTreeUnpackerRaw64>();
    } else {
      printf("Unsupported ttree output format %s\n", format.c_str());
    }
  } else if (kind == "rntuple") {
    if (format == "floats") {
      unpacker = std::make_unique<RNTupleUnpackerFloats>();
    } else if (format == "coll_float") {
      unpacker = std::make_unique<RNTupleUnpackerCollFloat>();
    } else if (format == "raw64") {
      unpacker = std::make_unique<RNTupleUnpackerRaw64>();
    } else {
      printf("Unsupported rntuple output format %s\n", format.c_str());
    }
  } else {
    printf("Unsupported kind %s\n", kind.c_str());
  }
  if (unpacker)
    unpacker->setCompression(compressionMethod, compressionLevel);
  return unpacker;
}

int singleCoreLiveUnpacker(const std::string &from,
                           const std::string &to,
                           const std::string &kind,
                           const std::string &format,
                           const std::string &compressionMethod,
                           int compressionLevel) {
  std::unique_ptr<UnpackerBase> unpacker = makeUnpacker(kind, format, compressionMethod, compressionLevel);
  if (!unpacker)
    return 1;

  TStopwatch timer;

  const unsigned int EVENT_SIZE = sizeof(struct inotify_event);
  const unsigned int BUF_LEN = 1024 * (EVENT_SIZE + 16);
  char buffer[BUF_LEN];

  int fd = inotify_init();

  if (fd < 0) {
    perror("inotify_init");
    return 1;
  }

  int wd = inotify_add_watch(fd, from.c_str(), IN_CLOSE_WRITE | IN_MOVED_TO);
  printf("Watching %s for new files\n", from.c_str());
  for (;;) {
    int length = read(fd, buffer, BUF_LEN);

    if (length < 0) {
      perror("read");
      return 2;
    }

    for (int i = 0; i < length;) {
      struct inotify_event *event = reinterpret_cast<inotify_event *>(&buffer[i]);
      if (event->len) {
       if (event->mask & (IN_CLOSE_WRITE | IN_MOVED_TO)) {
          if (!(event->mask & IN_ISDIR)) {
            std::string fname = event->name;
            if (fname.length() > 5 && fname.substr(fname.length() - 5) == ".dump") {
              std::vector<std::string> in(1, from + "/" + fname);
              if (std::filesystem::exists(in.front())) {
                std::string out = to + "/" + fname.substr(0, fname.length() - 5) + ".root";
                printf("Unpack %s -> %s\n", in.front().c_str(), out.c_str());
                timer.Start();
                unsigned long entries = unpacker->unpack(in, out);
                timer.Stop();
                double tcpu = timer.CpuTime(), treal = timer.RealTime();
                report(tcpu, treal, entries, in, out);
                unlink(in.front().c_str());
              }
            }
          }
        }
        i += EVENT_SIZE + event->len;
      }
    }
  }

  inotify_rm_watch(fd, wd);
  close(fd);

  return 0;
}

int main(int argc, char **argv) {
  if (argc < 5) {
    usage();
    return 1;
  }

  auto verbosity =
      ROOT::Experimental::RLogScopedVerbosity(ROOT::Experimental::NTupleLog(), ROOT::Experimental::ELogLevel::kError);

  std::string compressionMethod = "none";
  int compressionLevel = 0, threads = -1;
  while (1) {
    static struct option long_options[] = {{"help", no_argument, nullptr, 'h'},
                                           {"compression", required_argument, nullptr, 'z'},
                                           {nullptr, 0, nullptr, 0}};
    int option_index = 0;
    int optc = getopt_long(argc, argv, "hz:", long_options, &option_index);
    if (optc == -1)
      break;

    switch (optc) {
      case 'h':
        usage();
        return 0;
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
  std::string kind = std::string(argv[iarg]);
  std::string format = std::string(argv[iarg + 1]);
  std::string from = std::string(argv[iarg + 2]);
  std::string to = std::string(argv[iarg + 3]);
  printf("Will run %s format %s from %s to %s\n", argv[iarg], argv[iarg + 1], argv[iarg + 2], argv[iarg + 3]);
  return singleCoreLiveUnpacker(from, to, kind, format, compressionMethod, compressionLevel);
}