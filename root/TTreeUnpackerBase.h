#ifndef p2_clients_TTreeUnpackerBase_h
#define p2_clients_TTreeUnpackerBase_h
#include <TROOT.h>
#include <TTree.h>
#include <TFile.h>
#include "../UnpackerBase.h"
#include "unpack.h"

class TTreeUnpackerBase : public UnpackerBase {
public:
  TTreeUnpackerBase() : compressionAlgo_(ROOT::RCompressionSetting::EAlgorithm::kZLIB), compressionLevel_(0) {}
  ~TTreeUnpackerBase() override {}

  unsigned long int unpack(const std::vector<std::string> &ins, const std::string &out) const override = 0;
  void setThreads(unsigned int threads) override;
  void setCompression(const std::string &algo, unsigned int level) override;

protected:
  int compressionAlgo_, compressionLevel_;

  template <typename T, typename B, typename D>
  unsigned long int unpackBase(
      const std::vector<std::string> &ins, const std::string &out, T &data, B book, D decode) const {
    std::vector<std::fstream> fins;
    for (auto &in : ins) {
      fins.emplace_back(in, std::ios_base::in | std::ios_base::binary);
      if (!fins.back().good()) {
        throw std::runtime_error("Error opening " + in + " for otput");
      }
    }
    TFile *fout = nullptr;
    TTree *tree = nullptr;
    if (!out.empty()) {
      fout = TFile::Open(out.c_str(), "RECREATE", "", compressionLevel_);
      if (fout == nullptr || !fout) {
        throw std::runtime_error("Error opening " + out + " for otput");
      }
      if (compressionLevel_)
        fout->SetCompressionAlgorithm(compressionAlgo_);
      tree = new TTree("Events", "Events");
    }
    uint64_t header, payload[255];
    uint16_t run, bx, npuppi;
    uint32_t orbit;
    Bool_t good;
    if (tree) {
      // book common branches
      tree->Branch("run", &run, "run/s");
      tree->Branch("orbit", &orbit, "orbit/i");
      tree->Branch("bx", &bx, "bx/s");
      tree->Branch("good", &good, "good/O");
      // book custom banches
      book(tree, npuppi, header, payload, data);
    }
    // loop
    unsigned long int entries = 0;
    for (int ifile = 0, nfiles = fins.size(); fins[ifile].good(); ifile = (ifile == nfiles - 1 ? 0 : ifile + 1)) {
      std::fstream &fin = fins[ifile];
      do {
        fin.read(reinterpret_cast<char *>(&header), sizeof(uint64_t));
      } while (header == 0 && fin.good());
      if (!header)
        continue;
      parseHeader(header, run, bx, orbit, good, npuppi);
      if (npuppi)
        fin.read(reinterpret_cast<char *>(&payload[0]), npuppi * sizeof(uint64_t));
      decode(npuppi, payload, data);
      if (tree)
        tree->Fill();
      entries++;
    }
    // close
    if (tree) {
      tree->Write();
      fout->Close();
    }
    return entries;
  }
};

#endif