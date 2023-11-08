#include "TTreeUnpackerFloats.h"
#include <TTree.h>
#include "../unpack.h"

void TTreeUnpackerFloats::bookOutput(const std::string &out) {
  bookOutputBase(out);
  std::string F = (floatType_ == "float24" ? "f" : "F");
  if (tree_) {
    tree_->Branch("Puppi_pt", &data_.pt, ("Puppi_pt[nPuppi]/" + F).c_str());
    tree_->Branch("Puppi_eta", &data_.eta, ("Puppi_eta[nPuppi]/" + F).c_str());
    tree_->Branch("Puppi_phi", &data_.phi, ("Puppi_phi[nPuppi]/" + F).c_str());
    tree_->Branch("Puppi_pdgId", &data_.pdgid, "Puppi_pdgId[nPuppi]/S");
    tree_->Branch("Puppi_z0", &data_.z0, ("Puppi_z0[nPuppi]/" + F).c_str());
    tree_->Branch("Puppi_dxy", &data_.dxy, ("Puppi_dxy[nPuppi]/" + F).c_str());
    tree_->Branch("Puppi_wpuppi", &data_.wpuppi, ("Puppi_wpuppi[nPuppi]/" + F).c_str());
    tree_->Branch("Puppi_quality", &data_.quality, "Puppi_quality[nPuppi]/b");
  }
}

void TTreeUnpackerFloats::fillEvent(
    uint16_t run, uint32_t orbit, uint16_t bx, bool good, uint16_t nwords, const uint64_t *words) {
  run_ = run;
  orbit_ = orbit;
  bx_ = bx;
  good_ = good;
  npuppi_ = nwords;
  unpack_puppi_floats(
      nwords, words, data_.pt, data_.eta, data_.phi, data_.pdgid, data_.quality, data_.z0, data_.dxy, data_.wpuppi);
  if (tree_)
    tree_->Fill();
}
