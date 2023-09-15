#include "RNTupleUnpackerFloats.h"
#include "unpack.h"

unsigned long int RNTupleUnpackerFloats::unpack(const std::vector<std::string> &ins, const std::string &out) const {
  Data data;

  auto makeModel = [](ROOT::Experimental::RNTupleModel &model, Data &d) {
    d.p_npuppi = model.MakeField<uint8_t>("nPuppi");
    d.p_pt = model.MakeField<std::vector<float>>("Puppi_pt");
    d.p_eta = model.MakeField<std::vector<float>>("Puppi_eta");
    d.p_phi = model.MakeField<std::vector<float>>("Puppi_phi");
    d.p_pdgid = model.MakeField<std::vector<short int>>("Puppi_pdgId");
    d.p_z0 = model.MakeField<std::vector<float>>("Puppi_z0");
    d.p_dxy = model.MakeField<std::vector<float>>("Puppi_dxy");
    d.p_wpuppi = model.MakeField<std::vector<float>>("Puppi_wpuppi");
    d.p_quality = model.MakeField<std::vector<uint8_t>>("Puppi_quality");
  };

  auto fillModel = [](uint16_t &npuppi, uint64_t payload[255], Data &d) {
    *d.p_npuppi = npuppi;
    d.p_pt->resize(npuppi);
    d.p_eta->resize(npuppi);
    d.p_phi->resize(npuppi);
    d.p_pdgid->resize(npuppi);
    d.p_z0->resize(npuppi);
    d.p_dxy->resize(npuppi);
    d.p_wpuppi->resize(npuppi);
    d.p_quality->resize(npuppi);
    for (uint16_t i = 0; i < npuppi; ++i) {
      readshared(payload[i], (*d.p_pt)[i], (*d.p_eta)[i], (*d.p_phi)[i]);
      if (readpid(payload[i], (*d.p_pdgid)[i])) {
        readcharged(payload[i], (*d.p_z0)[i], (*d.p_dxy)[i], (*d.p_quality)[i]);
        (*d.p_wpuppi)[i] = 0;
      } else {
        readneutral(payload[i], (*d.p_wpuppi)[i], (*d.p_quality)[i]);
        (*d.p_z0)[i] = 0;
        (*d.p_dxy)[i] = 0;
      }
    }
  };

  return unpackBase(ins, out, data, makeModel, fillModel);
}