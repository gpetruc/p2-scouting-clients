#include "GMTTkMuRNTupleUnpackerCollFloat.h"
#include "../unpack.h"

void GMTTkMuRNTupleUnpackerCollFloat::bookOutput(const std::string &out) {
  auto model = modelBase();
  auto submodel = ROOT::Experimental::RNTupleModel::Create();
  data_.p_pt = submodel->MakeField<float>("pt");
  data_.p_eta = submodel->MakeField<float>("eta");
  data_.p_phi = submodel->MakeField<float>("phi");
  data_.p_charge = submodel->MakeField<int8_t>("charge");
  data_.p_z0 = submodel->MakeField<float>("z0");
  data_.p_d0 = submodel->MakeField<float>("d0");
  data_.p_beta = submodel->MakeField<float>("beta");
  data_.p_quality = submodel->MakeField<uint8_t>("quality");
  data_.p_isolation = submodel->MakeField<uint8_t>("isolation");
  data_.subwriter = model->MakeCollection("TkMu", std::move(submodel));
  bookBase(out, std::move(model));
}

void GMTTkMuRNTupleUnpackerCollFloat::fillEvent(
    uint16_t run, uint32_t orbit, uint16_t bx, bool good, uint16_t nwords, const uint64_t *words) {
  *dataBase_.p_run = run;
  *dataBase_.p_orbit = orbit;
  *dataBase_.p_bx = bx;
  *dataBase_.p_good = good;
  int nmu = (nwords * 2) / 3;
  const uint32_t *ptr32 = reinterpret_cast<const uint32_t *>(words);
  for (int i = 0; i < nmu; ++i, ptr32 += 3) {
    uint64_t wlo;
    uint32_t whi;
    if ((i & 1) == 0) {
      wlo = *reinterpret_cast<const uint64_t *>(ptr32);
      whi = *(ptr32 + 2);
    } else {
      wlo = *reinterpret_cast<const uint64_t *>(ptr32 + 1);
      whi = *ptr32;
    }
    *data_.p_pt = extractBitsFromW<1, 16>(wlo) * 0.03125f;
    *data_.p_phi = extractSignedBitsFromW<17, 13>(wlo) * float(M_PI / (1 << 12));
    *data_.p_eta = extractSignedBitsFromW<30, 14>(wlo) * float(M_PI / (1 << 12));
    *data_.p_z0 = extractSignedBitsFromW<44, 10>(wlo) * 0.05f;
    *data_.p_d0 = extractSignedBitsFromW<54, 10>(wlo) * 0.03f;
    *data_.p_charge = (whi & 1) ? -1 : +1;
    *data_.p_quality = extractBitsFromW<1, 8>(whi);
    *data_.p_isolation = extractBitsFromW<9, 4>(whi);
    *data_.p_beta = extractBitsFromW<13, 4>(whi) * 0.06f;
    if (writer_ && data_.subwriter)
      data_.subwriter->Fill();
  }
  if (writer_)
    writer_->Fill();
}