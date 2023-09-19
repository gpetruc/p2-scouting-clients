#include <cstdio>
#include <cstdlib>
#include <chrono>
#include <filesystem>
#include <errno.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/inotify.h>
#include "TTreeUnpackerFloats.h"
#include "TTreeUnpackerInts.h"
#include "TTreeUnpackerRaw64.h"
#include "RNTupleUnpackerFloats.h"
#include "RNTupleUnpackerCollFloat.h"
#include "RNTupleUnpackerRaw64.h"
#include <getopt.h>
#include <libgen.h>
#include <deque>
#include <tbb/pipeline.h>

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
  printf("  --delete        : delete unpacked output (for benchmarking without filling disks)\n");
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

struct Token {
  Token() : inputs(), output() {}
  Token(const std::string &inputName, const std::string &outputName) : inputs(1, inputName), output(outputName) {}
  Token(const std::vector<std::string> &inputNames, const std::string &outputName)
      : inputs(inputNames), output(outputName) {}
  std::vector<std::string> inputs;
  std::string output;
};

class Executor {
public:
  Executor(const UnpackerBase &unpacker, bool deleteAfterwards)
      : unpacker_(&unpacker), deleteAfterwards_(deleteAfterwards) {}
  Executor(const Executor &other) : unpacker_(other.unpacker_), deleteAfterwards_(other.deleteAfterwards_) {}
  void operator()(Token token) const {
    if (token.inputs.empty())
      return;
    printf("Unpack %s[#%d] -> %s\n", token.inputs.front().c_str(), int(token.inputs.size()), token.output.c_str());
    auto tstart = std::chrono::steady_clock::now();
    unsigned long entries = unpacker_->unpack(token.inputs, token.output);
    double dt = (std::chrono::duration<double>(std::chrono::steady_clock::now() - tstart)).count();
    report(dt, entries, token.inputs, token.output);
    for (const auto &in : token.inputs)
      unlink(in.c_str());
    if (deleteAfterwards_)
      unlink(token.output.c_str());
    else
      rename(token.output.c_str(), (token.output.substr(0, token.output.length() - 9) + ".root").c_str());
  }

private:
  const UnpackerBase *unpacker_;
  const bool deleteAfterwards_;
};

class Source {
public:
  Source(const std::string &from, const std::string &to, int inotify_fd)
      : from_(from), to_(to), inotify_fd_(inotify_fd) {}

  Token operator()(bool &stop) const {
    Token ret;
    if (workQueue_.empty()) {
      if (!readMessages()) {
        stop = true;
      }
    }
    if (!workQueue_.empty()) {
      ret = workQueue_.front();
      workQueue_.pop_front();
    }
    return ret;
  }
  Token operator()(tbb::flow_control &fc) const {
    bool stop = false;
    Token ret = operator()(stop);
    if (stop)
      fc.stop();
    return ret;
  }

private:
  static const unsigned int EVENT_SIZE = sizeof(struct inotify_event);
  static const unsigned int BUF_LEN = 1024 * (EVENT_SIZE + 16);
  const std::string from_, to_;
  const int inotify_fd_;

  mutable std::deque<Token> workQueue_;
  bool readMessages() const {
    char buffer[BUF_LEN];
    for (;;) {
      int length = read(inotify_fd_, buffer, BUF_LEN);

      if (length < 0) {
        perror("read");
        return false;
      }

      for (int i = 0; i < length;) {
        struct inotify_event *event = reinterpret_cast<inotify_event *>(&buffer[i]);
        if (event->len) {
          if (event->mask & (IN_CLOSE_WRITE | IN_MOVED_TO)) {
            if (!(event->mask & IN_ISDIR)) {
              std::string fname = event->name;
              if (fname.length() > 5 && fname.substr(fname.length() - 5) == ".dump") {
                std::string in(from_ + "/" + fname);
                if (std::filesystem::exists(in)) {
                  rename(in.c_str(), (in + ".taken").c_str());
                  in += ".taken";
                  std::string out = to_ + "/" + fname.substr(0, fname.length() - 5) + ".tmp.root";
                  bool found = false;
                  for (const auto &el : workQueue_) {
                    if (el.output == out) {
                      found = true;
                      break;
                    }
                  }
                  if (!found)
                    workQueue_.emplace_back(in, out);
                }
              }
            }
          }
          i += EVENT_SIZE + event->len;
        }
      }

      if (length > 0)
        return true;
    }

    return false;  // unreachable
  }
};

std::pair<int, int> initInotify(const std::string &from) {
  int fd = inotify_init();
  if (fd < 0) {
    perror("inotify_init");
    return std::make_pair(fd,0);
  }

  int wd = inotify_add_watch(fd, from.c_str(), IN_CLOSE_WRITE | IN_MOVED_TO);
  printf("Watching %s for new files\n", from.c_str());
  return std::make_pair(fd, wd);
}

int singleCoreLiveUnpacker(const UnpackerBase &unpacker,
                           const std::string &from,
                           const std::string &to,
                           bool deleteAfterwards) {
  auto fdwd = initInotify(from);
  if (fdwd.first < 0) {
    return -1;
  }

  auto src = Source(from, to, fdwd.first);
  auto dest = Executor(unpacker, deleteAfterwards);
  bool stop = false;
  while (!stop) {
    dest(src(stop));
  }

  inotify_rm_watch(fdwd.first, fdwd.second);
  close(fdwd.first);

  return 0;
}

int tbbLiveUnpacker(unsigned int threads,
                    const UnpackerBase &unpacker,
                           const std::string &from,
                    const std::string &to,
                    bool deleteAfterwards) {
  auto fdwd = initInotify(from);
  if (fdwd.first < 0) {
    return -1;
  }

  ROOT::EnableThreadSafety();

  auto head = tbb::make_filter<void, Token>(tbb::filter::serial_in_order, Source(from, to, fdwd.first));
  auto tail =
      tbb::make_filter<Token, void>((threads == 0 ? tbb::filter::serial_in_order : tbb::filter::parallel),
                              Executor(unpacker, deleteAfterwards));
  tbb::parallel_pipeline(std::max(1u, threads), head & tail);

  inotify_rm_watch(fdwd.first, fdwd.second);
  close(fdwd.first);

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
  bool deleteAfterwards = false;
  while (1) {
    static struct option long_options[] = {{"help", no_argument, nullptr, 'h'},
                                           {"threads", required_argument, nullptr, 'j'},
                                           {"compression", required_argument, nullptr, 'z'},
                                           {"delete", no_argument, nullptr, 'D'},
                                           {nullptr, 0, nullptr, 0}};
    int option_index = 0;
    int optc = getopt_long(argc, argv, "hz:j:D", long_options, &option_index);
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
      case 'j':
        threads = std::atoi(optarg);
        break;
      case 'D':
        deleteAfterwards = true;
        ;
        break;
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

  std::unique_ptr<UnpackerBase> unpacker = makeUnpacker(kind, format, compressionMethod, compressionLevel);
  if (!unpacker) return 2;
  if (threads == -1) {
    return singleCoreLiveUnpacker(*unpacker, from, to, deleteAfterwards);
  } else if (threads >= 0) {
    return tbbLiveUnpacker(threads, *unpacker, from, to, deleteAfterwards);
  }
}