#include "GMTTkMuRNTupleUnpackerCollFloat.h"
#include "../unpack.h"

UnpackerBase::Report GMTTkMuRNTupleUnpackerCollFloat::unpack(const std::vector<std::string> &ins,
                                                             const std::string &out) const {
  Data data;

  auto makeModel = [](ROOT::Experimental::RNTupleModel &model, Data &d) {
    auto submodel = ROOT::Experimental::RNTupleModel::Create();
    d.p_pt = submodel->MakeField<float>("pt");
    d.p_eta = submodel->MakeField<float>("eta");
    d.p_phi = submodel->MakeField<float>("phi");
    d.p_charge = submodel->MakeField<int8_t>("charge");
    d.p_z0 = submodel->MakeField<float>("z0");
    d.p_d0 = submodel->MakeField<float>("d0");
    d.p_beta = submodel->MakeField<float>("beta");
    d.p_quality = submodel->MakeField<uint8_t>("quality");
    d.p_isolation = submodel->MakeField<uint8_t>("isolation");
    d.subwriter = model.MakeCollection("TkMu", std::move(submodel));
  };

  auto fillModel = [](uint16_t &nwords, uint64_t payload[255], Data &d) {
    int nmu = (nwords * 2) / 3;
    const uint32_t *ptr32 = reinterpret_cast<const uint32_t *>(payload);
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
      *d.p_pt = extractBitsFromW<1, 16>(wlo) * 0.03125f;
      *d.p_phi = extractSignedBitsFromW<17, 13>(wlo) * float(M_PI / (1 << 12));
      *d.p_eta = extractSignedBitsFromW<30, 14>(wlo) * float(M_PI / (1 << 12));
      *d.p_z0 = extractSignedBitsFromW<44, 10>(wlo) * 0.05f;
      *d.p_d0 = extractSignedBitsFromW<54, 10>(wlo) * 0.03f;
      *d.p_charge = (whi & 1) ? -1 : +1;
      *d.p_quality = extractBitsFromW<1, 8>(whi);
      *d.p_isolation = extractBitsFromW<9, 4>(whi);
      *d.p_beta = extractBitsFromW<13, 4>(whi) * 0.06f;
      if (d.subwriter)
        d.subwriter->Fill();
    }
  };

  return unpackBase(ins, out, data, makeModel, fillModel);
}