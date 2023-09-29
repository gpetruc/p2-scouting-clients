#include "GMTTkMuTTreeUnpackerFloats.h"
#include <TTree.h>
#include "../unpack.h"

void GMTTkMuTTreeUnpackerFloats::bookOutput(const std::string &out) {
  bookOutputBase(out);
  std::string F = (floatType_ == "float24" ? "f" : "F");
  if (tree_) {
    tree_->Branch("nTkMu", &data_.nmu, "nTkMu/s");
    tree_->Branch("TkMu_pt", &data_.pt, ("TkMu_pt[nTkMu]/" + F).c_str());
    tree_->Branch("TkMu_eta", &data_.eta, ("TkMu_eta[nTkMu]/" + F).c_str());
    tree_->Branch("TkMu_phi", &data_.phi, ("TkMu_phi[nTkMu]/" + F).c_str());
    tree_->Branch("TkMu_charge", &data_.charge, "TkMu_charge[nTkMu]/B");
    tree_->Branch("TkMu_z0", &data_.z0, ("TkMu_z0[nTkMu]/" + F).c_str());
    tree_->Branch("TkMu_d0", &data_.d0, ("TkMu_d0[nTkMu]/" + F).c_str());
    tree_->Branch("TkMu_quality", &data_.quality, "TkMu_quality[nTkMu]/b");
    tree_->Branch("TkMu_isolation", &data_.isolation, "TkMu_isolation[nTkMu]/b");
    tree_->Branch("TkMu_beta", &data_.beta, ("TkMu_beta[nTkMu]/" + F).c_str());
  }
}

void GMTTkMuTTreeUnpackerFloats::fillEvent(
    uint16_t run, uint32_t orbit, uint16_t bx, bool good, uint16_t nwords, const uint64_t *words) {
  run_ = run;
  orbit_ = orbit;
  bx_ = bx;
  good_ = good;
  npuppi_ = nwords;
  decode_gmt_tkmu(nwords,
                  words,
                  data_.nmu,
                  data_.pt,
                  data_.eta,
                  data_.phi,
                  data_.charge,
                  data_.z0,
                  data_.d0,
                  data_.quality,
                  data_.isolation,
                  data_.beta);
  if (tree_)
    tree_->Fill();
}