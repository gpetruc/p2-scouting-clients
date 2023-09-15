#include "TTreeUnpackerFloats.h"
#include <TTree.h>
#include "unpack.h"

unsigned long int TTreeUnpackerFloats::unpack(const std::vector<std::string> &ins, const std::string &out) const {
  Data data;
  auto book = [&](TTree *tree, uint16_t &npuppi, uint64_t /*header*/, uint64_t /*payload*/[255], Data &d) {
    std::string F = (floatType_ == "float24" ? "f" : "F");
    tree->Branch("nPuppi", &npuppi, "nPuppi/s");
    tree->Branch("Puppi_pt", &d.pt, ("Puppi_pt[nPuppi]/" + F).c_str());
    tree->Branch("Puppi_eta", &d.eta, ("Puppi_eta[nPuppi]/" + F).c_str());
    tree->Branch("Puppi_phi", &d.phi, ("Puppi_phi[nPuppi]/" + F).c_str());
    tree->Branch("Puppi_pdgId", &d.pdgid, "Puppi_pdgId[nPuppi]/S");
    tree->Branch("Puppi_z0", &d.z0, ("Puppi_z0[nPuppi]/" + F).c_str());
    tree->Branch("Puppi_dxy", &d.dxy, ("Puppi_dxy[nPuppi]/" + F).c_str());
    tree->Branch("Puppi_wpuppi", &d.wpuppi, ("Puppi_wpuppi[nPuppi]/" + F).c_str());
    tree->Branch("Puppi_quality", &d.quality, "Puppi_quality[nPuppi]/b");
  };

  auto decode = [](uint16_t &npuppi, uint64_t payload[255], Data &d) {
    for (uint16_t i = 0; i < npuppi; ++i) {
      readshared(payload[i], d.pt[i], d.eta[i], d.phi[i]);
      if (readpid(payload[i], d.pdgid[i])) {
        readcharged(payload[i], d.z0[i], d.dxy[i], d.quality[i]);
        d.wpuppi[i] = 0;
      } else {
        readneutral(payload[i], d.wpuppi[i], d.quality[i]);
        d.z0[i] = 0;
        d.dxy[i] = 0;
      }
    }
  };

  return unpackBase(ins, out, data, book, decode);
}