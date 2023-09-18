#ifndef p2_clients_root_rdf_analysis_analysis_h
#define p2_clients_root_rdf_analysis_analysis_h

#include <vector>
#include <string>
#include "TFile.h"
#include "TH1.h"

class rdfAnalysis {
public:
  virtual ~rdfAnalysis() {}
  virtual void run(const std::string &informat,
                   const std::vector<std::string> &infiles,
                   const std::string &outformat,
                   const std::string &outfile) const = 0;
  static void saveHisto(TH1D *h, const std::string &outfile);
  static void saveRawHisto(TH1D *h, const std::string &outfile);
};

#endif