#ifndef p2_clients_root_rdf_analysis_analysis_h
#define p2_clients_root_rdf_analysis_analysis_h

#include <vector>
#include <string>
#include "TFile.h"
#include "TH1.h"

class rdfAnalysis {
public:
  virtual ~rdfAnalysis() {}
  struct Report {
    unsigned long int entries;
    float bytes_in;
    float bytes_out;
    float time;
    explicit Report(unsigned long int e, float t = 0, float i = 0, float o = 0)
        : entries(e), bytes_in(i), bytes_out(o), time(t) {}
    Report &operator+=(const Report &other) {
      entries += other.entries;
      bytes_in += other.bytes_in;
      bytes_out += other.bytes_out;
      time += other.time;
      return *this;
    }
  };
  virtual Report run(const std::string &informat,
                     const std::vector<std::string> &infiles,
                     const std::string &outformat,
                     const std::string &outfile) const = 0;
  static void saveHisto(TH1D *h, const std::string &outfile);
  static void saveRawHisto(TH1D *h, const std::string &outfile);
  static Report makeReport(float time,
                           unsigned long int entries,
                           const std::vector<std::string> &infiles,
                           const std::string &outfile);
};

#endif