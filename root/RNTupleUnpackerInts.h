#ifndef p2_clients_RNTupleUnpackerInts_h
#define p2_clients_RNTupleUnpackerInts_h
#include "RNTupleUnpackerBase.h"

class RNTupleUnpackerInts : public RNTupleUnpackerBase {
public:
  RNTupleUnpackerInts() {}
  ~RNTupleUnpackerInts() override {}

  Report unpack(const std::vector<std::string> &ins, const std::string &out) const override;

protected:
  struct Data {
    std::shared_ptr<uint8_t> p_npuppi;
    std::shared_ptr<std::vector<uint16_t>> p_pt;
    std::shared_ptr<std::vector<int16_t>> p_eta, p_phi;
    std::shared_ptr<std::vector<uint8_t>> p_pid, p_quality;
    std::shared_ptr<std::vector<int16_t>> p_z0;
    std::shared_ptr<std::vector<int8_t>> p_dxy;
    std::shared_ptr<std::vector<uint16_t>> p_wpuppi;
  };
};

#endif