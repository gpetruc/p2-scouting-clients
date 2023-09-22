#include "GMTTkMuTTreeUnpackerInts.h"
#include <TTree.h>
#include "../unpack.h"

UnpackerBase::Report GMTTkMuTTreeUnpackerInts::unpack(const std::vector<std::string> &ins,
                                                      const std::string &out) const {
  Data data;
  auto book = [=](TTree *tree, uint16_t & /*nwords*/, uint64_t /*header*/, uint64_t /*payload*/[255], Data &d) {
    tree->Branch("nTkMu", &d.nmu, "nTkMu/s");
    tree->Branch("TkMu_pt", &d.pt, "TkMu_pt[nTkMu]/s");
    tree->Branch("TkMu_eta", &d.eta, "TkMu_eta[nTkMu]/S");
    tree->Branch("TkMu_phi", &d.phi, "TkMu_phi[nTkMu]/S");
    tree->Branch("TkMu_charge", &d.charge, "TkMu_charge[nTkMu]/B");
    tree->Branch("TkMu_z0", &d.z0, "TkMu_z0[nTkMu]/S");
    tree->Branch("TkMu_d0", &d.d0, "TkMu_d0[nTkMu]/S");
    tree->Branch("TkMu_quality", &d.quality, "TkMu_quality[nTkMu]/b");
    tree->Branch("TkMu_isolation", &d.isolation, "TkMu_isolation[nTkMu]/b");
    tree->Branch("TkMu_beta", &d.beta, "TkMu_beta[nTkMu]/b");
  };

  auto decode = [](uint16_t &nwords, uint64_t payload[255], Data &d) {
    decode_gmt_tkmu(nwords, payload, d.nmu, d.pt, d.eta, d.phi, d.charge, d.z0, d.d0, d.quality, d.isolation, d.beta);
  };

  return unpackBase(ins, out, data, book, decode);
}