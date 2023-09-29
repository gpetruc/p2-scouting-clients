#include "RNTupleUnpackerRaw64.h"
#include <chrono>
#include "../unpack.h"

void RNTupleUnpackerRaw64::bookOutput(const std::string &out) {
  auto model = modelBase();
  p_data = model->MakeField<ROOT::RVec<uint64_t>>("Puppi_packed");
  bookBase(out, std::move(model));
}

void RNTupleUnpackerRaw64::fillEvent(
    uint16_t run, uint32_t orbit, uint16_t bx, bool good, uint16_t nwords, const uint64_t *words) {
  *dataBase_.p_run = run;
  *dataBase_.p_orbit = orbit;
  *dataBase_.p_bx = bx;
  *dataBase_.p_good = good;
  *p_data = ROOT::RVec<uint64_t>(const_cast<uint64_t *>(words), nwords);
  if (writer_)
    writer_->Fill();
}