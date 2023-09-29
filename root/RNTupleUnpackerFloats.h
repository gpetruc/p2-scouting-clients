#ifndef p2_clients_RNTupleUnpackerFloats_h
#define p2_clients_RNTupleUnpackerFloats_h
#include "RNTupleUnpackerBase.h"

class RNTupleUnpackerFloats : public RNTupleUnpackerBase {
public:
  RNTupleUnpackerFloats() {}
  ~RNTupleUnpackerFloats() override {}

  void bookOutput(const std::string &out) override final;

  void fillEvent(
      uint16_t run, uint32_t orbit, uint16_t bx, bool good, uint16_t nwords, const uint64_t *words) override final;

protected:
  struct Data {
    std::shared_ptr<uint8_t> p_npuppi;
    std::shared_ptr<std::vector<float>> p_pt, p_eta, p_phi, p_z0, p_dxy, p_wpuppi;
    std::shared_ptr<std::vector<short int>> p_pdgid;
    std::shared_ptr<std::vector<uint8_t>> p_quality;
  };

  Data data_;
};

#endif