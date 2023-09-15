#include "RNTupleUnpackerCollFloat.h"
#include "unpack.h"

unsigned long int RNTupleUnpackerCollFloat::unpack(const std::vector<std::string> &ins, const std::string &out) const {
  Data data;

  auto makeModel = [](ROOT::Experimental::RNTupleModel &model, Data &d) {
    auto submodel = ROOT::Experimental::RNTupleModel::Create();
    d.p_pt = submodel->MakeField<float>("pt");
    d.p_eta = submodel->MakeField<float>("eta");
    d.p_phi = submodel->MakeField<float>("phi");
    d.p_pdgid = submodel->MakeField<short int>("pdgId");
    d.p_z0 = submodel->MakeField<float>("z0");
    d.p_dxy = submodel->MakeField<float>("dxy");
    d.p_wpuppi = submodel->MakeField<float>("wpuppi");
    d.p_quality = submodel->MakeField<uint8_t>("quality");
    d.subwriter = model.MakeCollection("Puppi", std::move(submodel));
  };

  auto fillModel = [](uint16_t &npuppi, uint64_t payload[255], Data &d) {
    for (uint16_t i = 0; i < npuppi; ++i) {
      readshared(payload[i], *d.p_pt, *d.p_eta, *d.p_phi);
      if (readpid(payload[i], *d.p_pdgid)) {
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