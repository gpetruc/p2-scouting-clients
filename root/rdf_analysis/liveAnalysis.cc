#include <cstdio>
#include <cstdlib>
#include <chrono>
#include <atomic>
#include <filesystem>
#include <errno.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/inotify.h>
#include <getopt.h>
#include <deque>
#include <tbb/pipeline.h>
#include "analysis.h"
#include "w3piExample2022.h"

void usage() {
  printf("Usage: liveUnpacker.exe [ options ] analysis [arguments] /path/to/input /path/to/outputs \n");
  printf("  -f format: tree (default), mc, rntuple_(coll|vec)\n");
  printf("  -o output: none (default), histo, rawhisto, snapshot\n");
  printf("Options: \n");
  printf("  -j N : multithread with N threads\n");
  //printf("  -n N : wait for N files and analyze them together\n");
}

struct Totals {
  std::chrono::time_point<std::chrono::steady_clock> tstart;
  std::atomic<unsigned> jobs;
  std::atomic<unsigned long long> events, kb_in, kb_out;
  Totals() : jobs(0), events(0), kb_in(0), kb_out(0) {}
  void maybe_start() {
    if (jobs.load() == 0)
      tstart = std::chrono::steady_clock::now();
  }
  void add(const rdfAnalysis::Report &r, bool print = true) {
    jobs.fetch_add(1);
    events.fetch_add(r.entries);
    kb_in.fetch_add(std::round(r.bytes_in / 1024));
    kb_out.fetch_add(std::round(r.bytes_out / 1024));
    if (print) {
      auto tend = std::chrono::steady_clock::now();
      auto dt = (std::chrono::duration<double>(tend - tstart)).count();
      double GB_in = kb_in.load() / 1024.0 / 1024.0;
      double GB_out = kb_out.load() / 1024.0 / 1024.0;
      if (GB_out) {
        printf("Tot of %.1fs, %u jobs, %llu events, %.3f GB in (%.1f GB/s), %.3f GB out (%.1f GB/s)\n",
               dt,
               jobs.load(),
               events.load(),
               GB_in,
               GB_in / dt,
               GB_out,
               GB_out / dt);
      } else {
        printf("Tot of %.1fs, %u jobs, %llu events, %.3f GB in (%.1f GB/s)\n",
               dt,
               jobs.load(),
               events.load(),
               GB_in,
               GB_in / dt);
      }
    }
  }
};

class Executor {
public:
  Executor(const rdfAnalysis &analysis,
           const std::string &inFormat,
           const std::string &outFormat,
           const std::string &outPath,
           Totals &totals)
      : analysis_(&analysis), inFormat_(inFormat), outFormat_(outFormat), outPath_(outPath), totals_(&totals) {}
  void operator()(std::vector<std::string> inputs) const {
    if (inputs.empty())
      return;
    std::string output;
    if (!outFormat_.empty()) {
      output = outPath_ + "/" + inputs.front().substr(inputs.front().rfind('/') + 1);
      if (output.length() > 6 && output.substr(output.length() - 6) == ".taken")
        output = output.substr(0, output.length() - 6);
      if (output.length() > 5 && output.substr(output.length() - 5) == ".root") {
        if (outFormat_.substr(0, 3) == "raw") {
          output = output.substr(0, output.length() - 5) + ".raw";
        } else {
          output = output.substr(0, output.length() - 5) + ".tmp.root";
        }
      }
    }
    auto report = analysis_->run(inFormat_, inputs, outFormat_, output);
    if (output.empty()) {
      printf(
          "Run on %lu events in %.3f s (%.1f kHz, 40 MHz / %.1f); "
          "Size %.1f GB; Rate %.1f Gbps.\n",
          report.entries,
          report.time,
          report.entries * .001 / report.time,
          40e6 * report.time / report.entries,
          report.bytes_in / (1024. * 1024. * 1024.),
          report.bytes_in / (1024. * 1024. * 1024.) * 8 / report.time);
    } else {
      printf(
          "Run on %lu events in %.3f s (%.1f kHz, 40 MHz / %.1f); "
          "Size %.1f GB in, %.3f GB out; Rate %.1f Gbps in, %.2f Gbps out.\n",
          report.entries,
          report.time,
          report.entries * .001 / report.time,
          40e6 * report.time / report.entries,
          report.bytes_in / (1024. * 1024. * 1024.),
          report.bytes_out / (1024. * 1024. * 1024.),
          report.bytes_in / (1024. * 1024. * 1024.) * 8 / report.time,
          report.bytes_out / (1024. * 1024. * 1024.) * 8 / report.time);
    }
    totals_->add(report);
    for (const auto &in : inputs)
      std::filesystem::remove(in);
    if (output.length() > 9 && output.substr(output.length() - 9) == ".tmp.root") {
      if (std::filesystem::exists(output)) {
        std::filesystem::rename(output, (output.substr(0, output.length() - 9) + ".root"));
      }
    }
  }

private:
  const rdfAnalysis *analysis_;
  const std::string inFormat_, outFormat_, outPath_;
  mutable Totals *totals_;
};

class Source {
public:
  Source(const std::string &from, int inotify_fd, Totals &totals)
      : from_(from), inotify_fd_(inotify_fd), totals_(&totals) {}

  std::vector<std::string> operator()(tbb::flow_control &fc) const {
    std::vector<std::string> ret;
    if (workQueue_.empty()) {
      if (!readMessages()) {
        fc.stop();
      }
    }
    if (!workQueue_.empty()) {
      ret = workQueue_.front();
      workQueue_.pop_front();
    }
    totals_->maybe_start();
    return ret;
  }

private:
  static const unsigned int EVENT_SIZE = sizeof(struct inotify_event);
  static const unsigned int BUF_LEN = 1024 * (EVENT_SIZE + 16);
  const std::string from_, to_, ext_;
  const int inotify_fd_;
  mutable Totals *totals_;

  mutable std::deque<std::vector<std::string>> workQueue_;
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
              if (fname.length() > 5 && fname.substr(fname.length() - 5) == ".root" &&
                  (fname.length() < 9 || fname.substr(fname.length() - 9) == ".tmp.root")) {
                std::string in(from_ + "/" + fname);
                if (std::filesystem::exists(in)) {
                  std::filesystem::rename(in, in + ".taken");
                  in += ".taken";
                  std::vector<std::string> ins(1, in);
                  bool found = false;
                  for (const auto &el : workQueue_) {
                    if (el == ins) {
                      found = true;
                      break;
                    }
                  }
                  if (!found) {
                    workQueue_.emplace_back(ins);
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

int tbbLiveAnalysis(unsigned int threads,
                    const std::string &from,
                    const std::string &to,
                    const rdfAnalysis &analysis,
                    const std::string &inFormat,
                    const std::string &outFormat) {
  int fd = inotify_init();

  if (fd < 0) {
    perror("inotify_init");
    return 1;
  }

  int wd = inotify_add_watch(fd, from.c_str(), IN_CLOSE_WRITE | IN_MOVED_TO);
  printf("Watching %s for new files\n", from.c_str());

  ROOT::EnableThreadSafety();
  Totals totals;
  auto head = tbb::make_filter<void, std::vector<std::string>>(tbb::filter::serial_in_order, Source(from, fd, totals));
  auto queue = head & tbb::make_filter<std::vector<std::string>, void>(
                          (threads == 0 ? tbb::filter::serial_in_order : tbb::filter::parallel),
                          Executor(analysis, inFormat, outFormat, to, totals));
  tbb::parallel_pipeline(std::max(1u, threads), queue);

  inotify_rm_watch(fd, wd);
  close(fd);

  return 0;
}

int main(int argc, char **argv) {
  if (argc < 4) {
    usage();
    return 1;
  }

  std::string format = "tree", outputFormat = "none";
  int threads = 0;
  while (1) {
    static struct option long_options[] = {{"help", no_argument, nullptr, 'h'},
                                           {"format", required_argument, nullptr, 'f'},
                                           {"output", required_argument, nullptr, 'o'},
                                           {"threads", required_argument, nullptr, 'j'},
                                           {nullptr, 0, nullptr, 0}};
    int option_index = 0;
    int optc = getopt_long(argc, argv, "hf:o:j:", long_options, &option_index);
    if (optc == -1)
      break;

    switch (optc) {
      case 'h':
        usage();
        return 0;
      case 'f':
        format = std::string(optarg);
        break;
      case 'j':
        threads = std::atoi(optarg);
        break;
      case 'o':
        outputFormat = std::string(optarg);
        break;
      default:
        usage();
        return 1;
    }
  }

  int iarg = optind, narg = argc - optind;
  std::string analysis = std::string(argv[iarg++]);
  std::unique_ptr<rdfAnalysis> analyzer;
  if (analysis == "w3piExample2022") {
    std::string cuts = std::string(argv[iarg++]);
    if (cuts != "tight" && cuts != "loose") {
      printf("w3piExample2022 analysis requires to specify a set of cuts (\"tight\", \"loose\")\n");
      return 1;
    }
    analyzer = std::make_unique<w3piExample2022>(cuts, false);
    printf("Running analysis %s with cuts %s\n", analysis.c_str(), cuts.c_str());
  } else {
    printf("Unknown analysis %s\n", analysis.c_str());
    return 2;
  }
  std::string from = std::string(argv[iarg++]);
  std::string to = std::string(argv[iarg++]);
  printf("Will run from %s to %s\n", from.c_str(), to.c_str());

  return tbbLiveAnalysis(threads, from, to, *analyzer, format, outputFormat);
}