#include "GMTTkMuRNTupleUnpackerFloats.h"
#include "../unpack.h"

void GMTTkMuRNTupleUnpackerFloats::bookOutput(const std::string &out) {
  auto model = modelBase();
  data_.p_nmu = model->MakeField<uint8_t>("nTkMu");
  data_.p_pt = model->MakeField<std::vector<float>>("TkMu_pt");
  data_.p_eta = model->MakeField<std::vector<float>>("TkMu_eta");
  data_.p_phi = model->MakeField<std::vector<float>>("TkMu_phi");
  data_.p_charge = model->MakeField<std::vector<int8_t>>("TkMu_charge");
  data_.p_z0 = model->MakeField<std::vector<float>>("TkMu_z0");
  data_.p_d0 = model->MakeField<std::vector<float>>("TkMu_d0");
  data_.p_beta = model->MakeField<std::vector<float>>("TkMu_beta");
  data_.p_quality = model->MakeField<std::vector<uint8_t>>("TkMu_quality");
  data_.p_isolation = model->MakeField<std::vector<uint8_t>>("TkMu_isolation");
  bookBase(out, std::move(model));
}

void GMTTkMuRNTupleUnpackerFloats::fillEvent(
    uint16_t run, uint32_t orbit, uint16_t bx, bool good, uint16_t nwords, const uint64_t *words) {
  *dataBase_.p_run = run;
  *dataBase_.p_orbit = orbit;
  *dataBase_.p_bx = bx;
  *dataBase_.p_good = good;
  uint16_t nmu = (nwords * 2) / 3;
  *data_.p_nmu = nmu;
  data_.p_pt->resize(nmu);
  data_.p_eta->resize(nmu);
  data_.p_phi->resize(nmu);
  data_.p_charge->resize(nmu);
  data_.p_z0->resize(nmu);
  data_.p_d0->resize(nmu);
  data_.p_beta->resize(nmu);
  data_.p_quality->resize(nmu);
  data_.p_isolation->resize(nmu);
  decode_gmt_tkmu(nwords,
                  words,
                  nmu,
                  &data_.p_pt->front(),
                  &data_.p_eta->front(),
                  &data_.p_phi->front(),
                  &data_.p_charge->front(),
                  &data_.p_z0->front(),
                  &data_.p_d0->front(),
                  &data_.p_quality->front(),
                  &data_.p_isolation->front(),
                  &data_.p_beta->front());
  if (writer_)
    writer_->Fill();
}