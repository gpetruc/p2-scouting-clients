#ifndef p2_clients_GMTTkMuRNTupleUnpackerFloats_h
#define p2_clients_GMTTkMuRNTupleUnpackerFloats_h
#include "RNTupleUnpackerBase.h"

class GMTTkMuRNTupleUnpackerFloats : public RNTupleUnpackerBase {
public:
  GMTTkMuRNTupleUnpackerFloats() {}
  ~GMTTkMuRNTupleUnpackerFloats() override {}

  void bookOutput(const std::string &out) override final;

  void fillEvent(
      uint16_t run, uint32_t orbit, uint16_t bx, bool good, uint16_t nwords, const uint64_t *words) override final;

protected:
  struct Data {
    std::shared_ptr<uint8_t> p_nmu;
    std::shared_ptr<std::vector<float>> p_pt, p_eta, p_phi, p_z0, p_d0, p_beta;
    std::shared_ptr<std::vector<int8_t>> p_charge;
    std::shared_ptr<std::vector<uint8_t>> p_quality, p_isolation;
  };

  Data data_;
};

#endif