#include "RNTupleUnpackerCollInt.h"
#include "../unpack.h"

void RNTupleUnpackerCollInt::bookOutput(const std::string &out) {
  auto model = modelBase();
  auto submodel = ROOT::Experimental::RNTupleModel::Create();
  data_.p_pt = submodel->MakeField<uint16_t>("pt");
  data_.p_eta = submodel->MakeField<int16_t>("eta");
  data_.p_phi = submodel->MakeField<int16_t>("phi");
  data_.p_pid = submodel->MakeField<uint8_t>("pid");
  data_.p_z0 = submodel->MakeField<int16_t>("z0");
  data_.p_dxy = submodel->MakeField<int8_t>("dxy");
  data_.p_wpuppi = submodel->MakeField<uint16_t>("wpuppi");
  data_.p_quality = submodel->MakeField<uint8_t>("quality");
  data_.subwriter = model->MakeCollection("Puppi", std::move(submodel));
  bookBase(out, std::move(model));
}

void RNTupleUnpackerCollInt::fillEvent(
    uint16_t run, uint32_t orbit, uint16_t bx, bool good, uint16_t nwords, const uint64_t *words) {
  *dataBase_.p_run = run;
  *dataBase_.p_orbit = orbit;
  *dataBase_.p_bx = bx;
  *dataBase_.p_good = good;
  for (uint16_t i = 0; i < nwords; ++i) {
    readshared(words[i], *data_.p_pt, *data_.p_eta, *data_.p_phi);
    (*data_.p_pid) = (words[i] >> 37) & 0x7;
    if ((*data_.p_pid) > 1) {
      readcharged(words[i], *data_.p_z0, *data_.p_dxy, *data_.p_quality);
      *data_.p_wpuppi = 0;
    } else {
      readneutral(words[i], *data_.p_wpuppi, *data_.p_quality);
      *data_.p_z0 = 0;
      *data_.p_dxy = 0;
    }
    if (writer_ && data_.subwriter)
      data_.subwriter->Fill();
  }
  if (writer_)
    writer_->Fill();
}