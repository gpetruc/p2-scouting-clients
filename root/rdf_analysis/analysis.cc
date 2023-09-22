
#include "analysis.h"
#include "TFile.h"
#include "TH1.h"
#include <fstream>
#include <filesystem>

void rdfAnalysis::saveHisto(TH1D *h, const std::string &outfile) {
  TFile *fOut = TFile::Open(outfile.c_str(), "RECREATE", 0, 0);
  fOut->WriteTObject(h->Clone());
  fOut->Close();
}

void rdfAnalysis::saveRawHisto(TH1D *h, const std::string &outfile) {
  std::fstream f(outfile, std::ios::binary | std::ios::out | std::ios::trunc);
  uint32_t nbins = h->GetNbinsX();
  f.write(reinterpret_cast<char *>(&nbins), sizeof(uint32_t));
  f.write(reinterpret_cast<char *>(h->GetArray()), (nbins + 2) * sizeof(Double_t));
  f.close();
}

rdfAnalysis::Report rdfAnalysis::makeReport(float time,
                                            unsigned long int entries,
                                            const std::vector<std::string> &infiles,
                                            const std::string &outfile) {
  Report report(entries, time);
  try {
    unsigned long int insize = 0, outsize = 0;
    for (auto &in : infiles) {
      insize += std::filesystem::file_size(in);
    }
    if (!outfile.empty()) {
      outsize = std::filesystem::file_size(outfile);
    }
    report.bytes_in = insize;
    report.bytes_out = outsize;
  } catch (std::filesystem::filesystem_error &e) {
  }
  return report;
}