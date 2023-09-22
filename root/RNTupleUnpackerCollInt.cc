#include "RNTupleUnpackerCollInt.h"
#include "../unpack.h"

UnpackerBase::Report RNTupleUnpackerCollInt::unpack(const std::vector<std::string> &ins, const std::string &out) const {
  Data data;

  auto makeModel = [](ROOT::Experimental::RNTupleModel &model, Data &d) {
    auto submodel = ROOT::Experimental::RNTupleModel::Create();
    d.p_pt = submodel->MakeField<uint16_t>("pt");
    d.p_eta = submodel->MakeField<int16_t>("eta");
    d.p_phi = submodel->MakeField<int16_t>("phi");
    d.p_pid = submodel->MakeField<uint8_t>("pid");
    d.p_z0 = submodel->MakeField<int16_t>("z0");
    d.p_dxy = submodel->MakeField<int8_t>("dxy");
    d.p_wpuppi = submodel->MakeField<uint16_t>("wpuppi");
    d.p_quality = submodel->MakeField<uint8_t>("quality");
    d.subwriter = model.MakeCollection("Puppi", std::move(submodel));
  };

  auto fillModel = [](uint16_t &npuppi, uint64_t payload[255], Data &d) {
    for (uint16_t i = 0; i < npuppi; ++i) {
      readshared(payload[i], *d.p_pt, *d.p_eta, *d.p_phi);
      (*d.p_pid) = (payload[i] >> 37) & 0x7;
      if ((*d.p_pid) > 1) {
        readcharged(payload[i], *d.p_z0, *d.p_dxy, *d.p_quality);
        *d.p_wpuppi = 0;
      } else {
        readneutral(payload[i], *d.p_wpuppi, *d.p_quality);
        *d.p_z0 = 0;
        *d.p_dxy = 0;
      }
      if (d.subwriter)
        d.subwriter->Fill();
    }
  };

  return unpackBase(ins, out, data, makeModel, fillModel);
}