#include "GMTTkMuRNTupleUnpackerFloats.h"
#include "../unpack.h"

UnpackerBase::Report GMTTkMuRNTupleUnpackerFloats::unpack(const std::vector<std::string> &ins,
                                                          const std::string &out) const {
  Data data;

  auto makeModel = [](ROOT::Experimental::RNTupleModel &model, Data &d) {
    d.p_nmu = model.MakeField<uint8_t>("nTkMu");
    d.p_pt = model.MakeField<std::vector<float>>("TkMu_pt");
    d.p_eta = model.MakeField<std::vector<float>>("TkMu_eta");
    d.p_phi = model.MakeField<std::vector<float>>("TkMu_phi");
    d.p_charge = model.MakeField<std::vector<int8_t>>("TkMu_charge");
    d.p_z0 = model.MakeField<std::vector<float>>("TkMu_z0");
    d.p_d0 = model.MakeField<std::vector<float>>("TkMu_d0");
    d.p_beta = model.MakeField<std::vector<float>>("TkMu_beta");
    d.p_quality = model.MakeField<std::vector<uint8_t>>("TkMu_quality");
    d.p_isolation = model.MakeField<std::vector<uint8_t>>("TkMu_isolation");
  };

  auto fillModel = [](uint16_t &nwords, uint64_t payload[255], Data &d) {
    uint16_t nmu = (nwords * 2) / 3;
    *d.p_nmu = nmu;
    d.p_pt->resize(nmu);
    d.p_eta->resize(nmu);
    d.p_phi->resize(nmu);
    d.p_charge->resize(nmu);
    d.p_z0->resize(nmu);
    d.p_d0->resize(nmu);
    d.p_beta->resize(nmu);
    d.p_quality->resize(nmu);
    d.p_isolation->resize(nmu);
    decode_gmt_tkmu(nwords,
                    payload,
                    nmu,
                    &d.p_pt->front(),
                    &d.p_eta->front(),
                    &d.p_phi->front(),
                    &d.p_charge->front(),
                    &d.p_z0->front(),
                    &d.p_d0->front(),
                    &d.p_quality->front(),
                    &d.p_isolation->front(),
                    &d.p_beta->front());
  };

  return unpackBase(ins, out, data, makeModel, fillModel);
}