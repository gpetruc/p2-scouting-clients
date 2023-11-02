#include "RNTupleUnpackerCollFloat.h"
#include "../unpack.h"

void RNTupleUnpackerCollFloat::bookOutput(const std::string &out) {
  auto model = modelBase();
  auto submodel = ROOT::Experimental::RNTupleModel::Create();
  data_.p_pt = submodel->MakeField<float>("pt");
  data_.p_eta = submodel->MakeField<float>("eta");
  data_.p_phi = submodel->MakeField<float>("phi");
  data_.p_pdgid = submodel->MakeField<short int>("pdgId");
  data_.p_z0 = submodel->MakeField<float>("z0");
  data_.p_dxy = submodel->MakeField<float>("dxy");
  data_.p_wpuppi = submodel->MakeField<float>("wpuppi");
  data_.p_quality = submodel->MakeField<uint8_t>("quality");
  data_.subwriter = model->MakeCollection("Puppi", std::move(submodel));
  bookBase(out, std::move(model));
}

void RNTupleUnpackerCollFloat::fillEvent(
    uint16_t run, uint32_t orbit, uint16_t bx, bool good, uint16_t nwords, const uint64_t *words) {
  *dataBase_.p_run = run;
  *dataBase_.p_orbit = orbit;
  *dataBase_.p_bx = bx;
  *dataBase_.p_good = good;
  for (uint16_t i = 0; i < nwords; ++i) {
    readshared(words[i], *data_.p_pt, *data_.p_eta, *data_.p_phi);
    if (readpid(words[i], *data_.p_pdgid)) {
      readcharged(words[i], *data_.p_z0, *data_.p_dxy, *data_.p_quality);
      *data_.p_wpuppi = 1;
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