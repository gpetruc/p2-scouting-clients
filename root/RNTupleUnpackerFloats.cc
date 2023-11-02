#include "RNTupleUnpackerFloats.h"
#include "../unpack.h"

void RNTupleUnpackerFloats::bookOutput(const std::string &out) {
  auto model = modelBase();
  data_.p_npuppi = model->MakeField<uint8_t>("nPuppi");
  data_.p_pt = model->MakeField<std::vector<float>>("Puppi_pt");
  data_.p_eta = model->MakeField<std::vector<float>>("Puppi_eta");
  data_.p_phi = model->MakeField<std::vector<float>>("Puppi_phi");
  data_.p_pdgid = model->MakeField<std::vector<short int>>("Puppi_pdgId");
  data_.p_z0 = model->MakeField<std::vector<float>>("Puppi_z0");
  data_.p_dxy = model->MakeField<std::vector<float>>("Puppi_dxy");
  data_.p_wpuppi = model->MakeField<std::vector<float>>("Puppi_wpuppi");
  data_.p_quality = model->MakeField<std::vector<uint8_t>>("Puppi_quality");
  bookBase(out, std::move(model));
}

void RNTupleUnpackerFloats::fillEvent(
    uint16_t run, uint32_t orbit, uint16_t bx, bool good, uint16_t nwords, const uint64_t *words) {
  *dataBase_.p_run = run;
  *dataBase_.p_orbit = orbit;
  *dataBase_.p_bx = bx;
  *dataBase_.p_good = good;
  *data_.p_npuppi = nwords;
  data_.p_pt->resize(nwords);
  data_.p_eta->resize(nwords);
  data_.p_phi->resize(nwords);
  data_.p_pdgid->resize(nwords);
  data_.p_z0->resize(nwords);
  data_.p_dxy->resize(nwords);
  data_.p_wpuppi->resize(nwords);
  data_.p_quality->resize(nwords);
  for (uint16_t i = 0; i < nwords; ++i) {
    readshared(words[i], (*data_.p_pt)[i], (*data_.p_eta)[i], (*data_.p_phi)[i]);
    if (readpid(words[i], (*data_.p_pdgid)[i])) {
      readcharged(words[i], (*data_.p_z0)[i], (*data_.p_dxy)[i], (*data_.p_quality)[i]);
      (*data_.p_wpuppi)[i] = 1;
    } else {
      readneutral(words[i], (*data_.p_wpuppi)[i], (*data_.p_quality)[i]);
      (*data_.p_z0)[i] = 0;
      (*data_.p_dxy)[i] = 0;
    }
  }
  if (writer_)
    writer_->Fill();
}