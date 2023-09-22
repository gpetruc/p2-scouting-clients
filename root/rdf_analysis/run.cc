#include "analysis.h"
#include "w3piExample2022.h"

#include <getopt.h>

void usage() {
  printf("Usage: run.exe [options] analysis [arguments] infile(s) outfile\n");
  printf("  -f format: tree (default), mc, rntuple_(coll|vec)\n");
  printf("  -o output: none (default), histo, rawhisto, snapshot\n");
}

int main(int argc, char **argv) {
  if (argc <= 1) {
    usage();
    return 1;
  }

  int threads = -1;
  bool verbose = false;
  std::string format = "tree", outputFormat = "none";
  while (1) {
    static struct option long_options[] = {{"help", no_argument, nullptr, 'h'},
                                           {"verbose", no_argument, nullptr, 'v'},
                                           {"format", required_argument, nullptr, 'f'},
                                           {"output", required_argument, nullptr, 'o'},
                                           {"threads", required_argument, nullptr, 'j'},
                                           {nullptr, 0, nullptr, 0}};
    int option_index = 0;
    int optc = getopt_long(argc, argv, "hvj:f:o:", long_options, &option_index);
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
      case 'v':
        verbose = true;
        break;
      default:
        usage();
        return 1;
    }
  }

  int iarg = optind, narg = argc - optind;

  std::string analysis = argv[iarg++];
  std::vector<std::string> ins;
  while (iarg < argc) {
    ins.emplace_back(argv[iarg++]);
  }

  std::string output = "";
  if (outputFormat != "none") {
    if (ins.size() < 2) {
      printf("Error: specifying an output format, you must specify an output filename.\n");
      return 1;
    }
    output = ins.back();
    ins.pop_back();
  }

  if (threads > -1)
    ROOT::EnableImplicitMT(threads);

  std::unique_ptr<rdfAnalysis> analyzer;
  if (analysis == "w3piExample2022") {
    if (ins.size() <= 1 || (ins.front() != "tight" && ins.front() != "loose")) {
      printf("w3piExample2022 analysis requires to specify a set of cuts (\"tight\", \"loose\")\n");
      return 1;
    }
    std::string cuts = ins.front();
    ins.erase(ins.begin());
    analyzer = std::make_unique<w3piExample2022>(cuts, verbose);
    printf("Running analysis %s with cuts %s\n", analysis.c_str(), cuts.c_str());
  } else {
    printf("Unknown analysis %s\n", analysis.c_str());
    return 2;
  }

  auto report = analyzer->run(format, ins, outputFormat, output);
  if (output.empty()) {
    printf(
        "Run on %d files, %lu events in %.3f s (%.1f kHz, 40 MHz / %.1f); "
        "Size %.1f GB; Rate %.1f Gbps.\n",
        int(ins.size()),
        report.entries,
        report.time,
        report.entries * .001 / report.time,
        40e6 * report.time / report.entries,
        report.bytes_in / (1024. * 1024. * 1024.),
        report.bytes_in / (1024. * 1024. * 1024.) * 8 / report.time);
  } else {
    printf(
        "Run on %d files, %lu events in %.3f s (%.1f kHz, 40 MHz / %.1f); "
        "Size %.1f GB in, %.3f GB out; Rate %.1f Gbps in, %.2f Gbps out.\n",
        int(ins.size()),
        report.entries,
        report.time,
        report.entries * .001 / report.time,
        40e6 * report.time / report.entries,
        report.bytes_in / (1024. * 1024. * 1024.),
        report.bytes_out / (1024. * 1024. * 1024.),
        report.bytes_in / (1024. * 1024. * 1024.) * 8 / report.time,
        report.bytes_out / (1024. * 1024. * 1024.) * 8 / report.time);
  }
}
