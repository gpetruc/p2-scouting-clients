#include "TTreeUnpackerRaw64.h"
#include <TTree.h>
#include "../unpack.h"

void TTreeUnpackerRaw64::bookOutput(const std::string &out) {
  bookOutputBase(out);
  uint64_t dummy[1];
  if (tree_) {
    branch_ = tree_->Branch("Puppi_packed", dummy, "Puppi_packed[nPuppi]/l");
  }
}

void TTreeUnpackerRaw64::fillEvent(
    uint16_t run, uint32_t orbit, uint16_t bx, bool good, uint16_t nwords, const uint64_t *words) {
  run_ = run;
  orbit_ = orbit;
  bx_ = bx;
  good_ = good;
  npuppi_ = nwords;
  if (tree_) {
    branch_->SetAddress((void *)words);  // bad TTree API, should be const void *
    tree_->Fill();
  }
}