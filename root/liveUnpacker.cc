#include <cstdio>
#include <cstdlib>
#include <chrono>
#include <filesystem>
#include <errno.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/inotify.h>
#include <getopt.h>
#include <libgen.h>
#include <deque>
#include <tbb/pipeline.h>
#include <atomic>
#include <cmath>
#include <TROOT.h>
#include <TFile.h>
#include <ROOT/RLogger.hxx>
#include <ROOT/RNTuple.hxx>
#include "RootUnpackMaker.h"
#include "../unpack.h"
#include <getopt.h>

void usage() {
  printf("Usage: liveUnpacker.exe [ options ] <obj> <kind> <format> /path/to/input /path/to/outputs \n");
  printf("  obj  := puppi | tkmu \n");
  printf("  kind := ttree | rntuple\n");
  printf("  puppi ttree  formats   := float | float24 | int | raw64\n");
  printf("  puppi rntule formats   := floats | coll_float | ints | coll_int | raw64\n");
  printf("  tkmu  ttree  formats   := float | float24 | int\n");
  printf("  tkmu  rntule formats   := floats | coll_float\n");
  printf("Options: \n");
  printf("  --demux N       : demux N timeslices\n");
  printf("  -j N            : multithread with N threads\n");
  printf("  -z algo[,level] : enable compression\n");
  printf("                    algorithms supported are none, lzma, zlib, lz4, zstd;\n");
  printf("                    default level is 4\n");
  printf("  --delete        : delete unpacked output (for benchmarking without filling disks)\n");
  printf("  --orbitBitsPerLS N : log2(Orbits) per lumisection (default 18, i.e. 256k orbits per 23s lumisection)\n");
}

struct Token {
  Token() : inputs(), output() {}
  Token(const std::string &inputName, const std::string &outputName) : inputs(1, inputName), output(outputName) {}
  Token(const std::vector<std::string> &inputNames, const std::string &outputName)
      : inputs(inputNames), output(outputName) {}
  std::vector<std::string> inputs;
  std::string output;
};

struct Totals {
  std::chrono::time_point<std::chrono::steady_clock> tstart;
  std::atomic<unsigned> jobs;
  std::atomic<unsigned long long> events, kb_in, kb_out;
  Totals() : jobs(0), events(0), kb_in(0), kb_out(0) {}
  void maybe_start() {
    if (jobs.load() == 0)
      tstart = std::chrono::steady_clock::now();
  }
  void add(const UnpackerBase::Report &r, bool print = true) {
    jobs.fetch_add(1);
    events.fetch_add(r.entries);
    kb_in.fetch_add(std::round(r.bytes_in / 1024));
    kb_out.fetch_add(std::round(r.bytes_out / 1024));
    if (print) {
      auto tend = std::chrono::steady_clock::now();
      auto dt = (std::chrono::duration<double>(tend - tstart)).count();
      double GB_in = kb_in.load() / 1024.0 / 1024.0;
      double GB_out = kb_out.load() / 1024.0 / 1024.0;
      printf("Tot of %.1fs, %u jobs, %llu events, %.3f GB in (%.1f GB/s), %.3f GB out (%.1f GB/s)\n",
             dt,
             jobs.load(),
             events.load(),
             GB_in,
             GB_in / dt,
             GB_out,
             GB_out / dt);
    }
  }
};

class Executor {
public:
  Executor(const RootUnpackMaker::Spec &unpackerSpec, Totals &totals, bool deleteAfterwards)
      : spec_(unpackerSpec), totals_(&totals), deleteAfterwards_(deleteAfterwards) {}
  Executor(const Executor &other)
      : spec_(other.spec_), totals_(other.totals_), deleteAfterwards_(other.deleteAfterwards_) {}
  void operator()(Token token) const {
    if (token.inputs.empty())
      return;
    std::unique_ptr<UnpackerBase> unpacker = RootUnpackMaker::make(spec_);
    printf("Unpack %s[#%d] -> %s\n", token.inputs.front().c_str(), int(token.inputs.size()), token.output.c_str());
    auto report = unpacker->unpackFiles(token.inputs, token.output);
    printReport(report);
    totals_->add(report);
    for (const auto &in : token.inputs)
      unlink(in.c_str());
    if (deleteAfterwards_)
      unlink(token.output.c_str());
    else
      rename(token.output.c_str(), (token.output.substr(0, token.output.length() - 9) + ".root").c_str());
  }

private:
  RootUnpackMaker::Spec spec_;
  mutable Totals *totals_;
  const bool deleteAfterwards_;
};

class Source {
public:
  Source(const std::string &from,
         const std::string &to,
         unsigned int timeslices,
         unsigned int orbitBitsPerLS,
         int inotify_fd,
         Totals &totals)
      : from_(from),
        to_(to),
        inotify_fd_(inotify_fd),
        timeslices_(timeslices),
        orbitBitsPerLS_(orbitBitsPerLS),
        totals_(&totals) {}

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
    totals_->maybe_start();
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
  unsigned int timeslices_, orbitBitsPerLS_;
  mutable Totals *totals_;

  bool parseUnpackedFilename(const std::string &fname, unsigned int &orbit, unsigned int &ts, std::string &fout) const {
    // in the form run000037_ls0001_index000513[_ts00].raw
    if (fname.length() < 5)
      return false;
    std::string work = fname.substr(0, fname.length() - 4);  // remove the ".raw";
    auto pos = work.rfind('_');
    if (pos == std::string::npos || (work.substr(pos, 3) != "_ts")) {
      return false;
    }
    ts = std::atoi(work.substr(pos + 3).c_str());
    work = work.substr(0, pos);
    pos = work.rfind('_');
    if (pos == std::string::npos || work.substr(pos, 6) != "_index") {
      return false;
    }
    unsigned int index = std::atoi(work.substr(pos + 6).c_str());
    auto pos2 = work.rfind('_', pos - 1);
    if (pos2 == std::string::npos || work.substr(pos2, 3) != "_ls") {
      return false;
    }
    unsigned int ls = std::atoi(work.substr(pos2 + 3, pos).c_str());
    orbit = (std::max(ls - 1, 0u) << orbitBitsPerLS_) + index;
    fout = work + ".tmp.root";
    // printf("parseUnpF('%s') --> orbit %u, ls %u, ts %u, fout '%s'\n", work.c_str(), orbit, ls, ts, fout.c_str());
    return true;
  }
  struct DemuxQueueItem {
    unsigned int orbit;
    std::vector<std::string> timeslices;
    std::string fout;
    DemuxQueueItem(unsigned int orbit_, unsigned int ntimeslices, const std::string &fout_)
        : orbit(orbit_), timeslices(ntimeslices), fout(fout_) {}
  };
  mutable std::list<DemuxQueueItem> demuxQueue_;
  mutable std::deque<Token> workQueue_;
  void maybeAddSingle(const std::string &fname, std::string &in) const {
    std::filesystem::rename(in, in + ".taken");
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
  void maybeAddTMux(const std::string &fname, std::string &in) const {
    unsigned int orbit;
    unsigned int ts;
    std::string fout;
    if (!parseUnpackedFilename(fname, orbit, ts, fout))
      return;
    bool found = false;
    for (auto it = demuxQueue_.begin(), ed = demuxQueue_.end(); it != ed; ++it) {
      if (it->orbit != orbit)
        continue;
      assert(ts < timeslices_ && it->timeslices.size() == timeslices_);
      //printf("Found demuxQueue record for orbit %u\n", orbit);
      //or (unsigned int i = 0; i < timeslices_; ++i) {
      //  printf("  ts[%u] = '%s'\n", i, it->timeslices[i].c_str());
      //}
      if (it->timeslices[ts].empty()) {
        it->timeslices[ts] = in;
        bool complete = true;
        for (const auto &t : it->timeslices) {
          if (t.empty())
            complete = false;
        }
        if (complete) {
          //printf("demuxQueue record for orbit %u has all %u timeslices available.\n", orbit, timeslices_);
          for (auto &i : it->timeslices) {
            std::filesystem::rename(i, i + ".taken");
            i += ".taken";
          }
          workQueue_.emplace_back(it->timeslices, to_ + "/" + it->fout);
          demuxQueue_.erase(it);
        }
      } else {
        if (it->timeslices[ts] != in) {
          printf("ERROR, mismatch files for orbit %d, ts %d: %s vs %s\n",
                 orbit,
                 ts,
                 it->timeslices[ts].c_str(),
                 in.c_str());
          abort();
        }
      }
      found = true;
      break;
    }
    if (!found) {
      //printf("Creating new demuxQueue record for orbit %u, with ts %u\n", orbit, ts);
      auto &el = demuxQueue_.emplace_back(orbit, timeslices_, fout);
      el.timeslices[ts] = in;
    }
  }
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
              if (fname.length() > 4 && fname.substr(fname.length() - 4) == ".raw") {
                std::string in(from_ + "/" + fname);
                if (std::filesystem::exists(in)) {
                  if (timeslices_ == 0) {
                    maybeAddSingle(fname, in);
                  } else {
                    maybeAddTMux(fname, in);
                  }
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
    return std::make_pair(fd, 0);
  }

  int wd = inotify_add_watch(fd, from.c_str(), IN_CLOSE_WRITE | IN_MOVED_TO);
  printf("Watching %s for new files\n", from.c_str());
  return std::make_pair(fd, wd);
}

int singleCoreLiveUnpacker(const RootUnpackMaker::Spec &unpackerSpec,
                           const std::string &from,
                           const std::string &to,
                           unsigned int timeslices,
                           unsigned int orbitBitsPerLS,
                           bool deleteAfterwards) {
  auto fdwd = initInotify(from);
  if (fdwd.first < 0) {
    return -1;
  }

  Totals totals;
  auto src = Source(from, to, timeslices, orbitBitsPerLS, fdwd.first, totals);
  auto dest = Executor(unpackerSpec, totals, deleteAfterwards);
  bool stop = false;
  while (!stop) {
    dest(src(stop));
  }

  inotify_rm_watch(fdwd.first, fdwd.second);
  close(fdwd.first);

  return 0;
}

int tbbLiveUnpacker(unsigned int threads,
                    const RootUnpackMaker::Spec &unpackerSpec,
                    const std::string &from,
                    const std::string &to,
                    unsigned int timeslices,
                    unsigned int orbitBitsPerLS,
                    bool deleteAfterwards) {
  auto fdwd = initInotify(from);
  if (fdwd.first < 0) {
    return -1;
  }

  ROOT::EnableThreadSafety();

  Totals totals;
  auto head = tbb::make_filter<void, Token>(tbb::filter::serial_in_order,
                                            Source(from, to, timeslices, orbitBitsPerLS, fdwd.first, totals));
  auto tail = tbb::make_filter<Token, void>((threads == 0 ? tbb::filter::serial_in_order : tbb::filter::parallel),
                                            Executor(unpackerSpec, totals, deleteAfterwards));
  tbb::parallel_pipeline(std::max(1u, threads), head & tail);

  inotify_rm_watch(fdwd.first, fdwd.second);
  close(fdwd.first);

  return 0;
}

int main(int argc, char **argv) {
  if (argc < 6) {
    usage();
    return 1;
  }

  auto verbosity =
      ROOT::Experimental::RLogScopedVerbosity(ROOT::Experimental::NTupleLog(), ROOT::Experimental::ELogLevel::kError);

  std::string compressionMethod = "none";
  int compressionLevel = 0, threads = -1;
  unsigned int timeslices = 0, orbitBitsPerLS = 18;
  bool deleteAfterwards = false;
  while (1) {
    static struct option long_options[] = {{"help", no_argument, nullptr, 'h'},
                                           {"threads", required_argument, nullptr, 'j'},
                                           {"demux", required_argument, nullptr, 'd'},
                                           {"orbitBitsPerLS", required_argument, nullptr, 1},
                                           {"compression", required_argument, nullptr, 'z'},
                                           {"delete", no_argument, nullptr, 'D'},
                                           {nullptr, 0, nullptr, 0}};
    int option_index = 0;
    int optc = getopt_long(argc, argv, "hz:j:d:D", long_options, &option_index);
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
      case 'd':
        timeslices = std::atoi(optarg);
        break;
      case 1:
        orbitBitsPerLS = std::atoi(optarg);
        assert(orbitBitsPerLS > 0);
        break;
      case 'D':
        deleteAfterwards = true;
        break;
      default:
        usage();
        return 1;
    }
  }

  int iarg = optind, narg = argc - optind;
  if (narg != 5) {
    usage();
    return 1;
  }
  std::string obj = std::string(argv[iarg]);
  std::string kind = std::string(argv[iarg + 1]);
  std::string format = std::string(argv[iarg + 2]);
  std::string from = std::string(argv[iarg + 3]);
  std::string to = std::string(argv[iarg + 4]);
  printf("Will run %s %s format %s from %s to %s\n",
         argv[iarg],
         argv[iarg + 1],
         argv[iarg + 2],
         argv[iarg + 3],
         argv[iarg + 4]);

  RootUnpackMaker::Spec spec(obj, kind, format, compressionMethod, compressionLevel);
  if (threads == -1) {
    return singleCoreLiveUnpacker(spec, from, to, timeslices, orbitBitsPerLS, deleteAfterwards);
  } else if (threads >= 0) {
    return tbbLiveUnpacker(threads, spec, from, to, timeslices, orbitBitsPerLS, deleteAfterwards);
  }
}