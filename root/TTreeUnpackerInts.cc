#include "TTreeUnpackerInts.h"
#include <TTree.h>
#include "../unpack.h"

void TTreeUnpackerInts::bookOutput(const std::string &out) {
  bookOutputBase(out);
  if (tree_) {
    tree_->Branch("Puppi_pt", &data_.pt, "Puppi_pt[nPuppi]/s");
    tree_->Branch("Puppi_eta", &data_.eta, "Puppi_eta[nPuppi]/S");
    tree_->Branch("Puppi_phi", &data_.phi, "Puppi_phi[nPuppi]/S");
    tree_->Branch("Puppi_pid", &data_.pid, "Puppi_pid[nPuppi]/b");
    tree_->Branch("Puppi_z0", &data_.z0, "Puppi_z0[nPuppi]/S");
    tree_->Branch("Puppi_dxy", &data_.dxy, "Puppi_dxy[nPuppi]/B");
    tree_->Branch("Puppi_quality", &data_.quality, "Puppi_quality[nPuppi]/b");
    tree_->Branch("Puppi_wpuppi", &data_.wpuppi, "Puppi_wpuppi[nPuppi]/s");
  }
}

void TTreeUnpackerInts::fillEvent(
    uint16_t run, uint32_t orbit, uint16_t bx, bool good, uint16_t nwords, const uint64_t *words) {
  run_ = run;
  orbit_ = orbit;
  bx_ = bx;
  good_ = good;
  npuppi_ = nwords;
  unpack_puppi_ints(
      nwords, words, data_.pt, data_.eta, data_.phi, data_.pid, data_.quality, data_.z0, data_.dxy, data_.wpuppi);
  if (tree_)
    tree_->Fill();
}