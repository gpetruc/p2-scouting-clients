#ifndef p2_clients_GMTTkMuRNTupleUnpackerFloats_h
#define p2_clients_GMTTkMuRNTupleUnpackerFloats_h
#include "RNTupleUnpackerBase.h"

class GMTTkMuRNTupleUnpackerFloats : public RNTupleUnpackerBase {
public:
  GMTTkMuRNTupleUnpackerFloats() {}
  ~GMTTkMuRNTupleUnpackerFloats() override {}

  Report unpack(const std::vector<std::string> &ins, const std::string &out) const override;

protected:
  struct Data {
    std::shared_ptr<uint8_t> p_nmu;
    std::shared_ptr<std::vector<float>> p_pt, p_eta, p_phi, p_z0, p_d0, p_beta;
    std::shared_ptr<std::vector<int8_t>> p_charge;
    std::shared_ptr<std::vector<uint8_t>> p_quality, p_isolation;
  };
};

#endif