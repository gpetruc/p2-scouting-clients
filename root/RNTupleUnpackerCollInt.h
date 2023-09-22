#ifndef p2_clients_RNTupleUnpackerCollInt_h
#define p2_clients_RNTupleUnpackerCollInt_h
#include "RNTupleUnpackerBase.h"

class RNTupleUnpackerCollInt : public RNTupleUnpackerBase {
public:
  RNTupleUnpackerCollInt() {}
  ~RNTupleUnpackerCollInt() override {}

  Report unpack(const std::vector<std::string> &ins, const std::string &out) const override;

protected:
  struct Data {
    std::shared_ptr<uint16_t> p_pt;
    std::shared_ptr<int16_t> p_eta, p_phi;
    std::shared_ptr<uint8_t> p_pid, p_quality;
    std::shared_ptr<int16_t> p_z0;
    std::shared_ptr<int8_t> p_dxy;
    std::shared_ptr<uint16_t> p_wpuppi;
    std::shared_ptr<ROOT::Experimental::RCollectionNTupleWriter> subwriter;
  };
};

#endif