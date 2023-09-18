
#include "analysis.h"
#include "TFile.h"
#include "TH1.h"
#include <fstream>

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