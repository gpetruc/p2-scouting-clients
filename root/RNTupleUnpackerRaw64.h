#ifndef p2_clients_RNTupleUnpackerRaw64_h
#define p2_clients_RNTupleUnpackerRaw64_h
#include "RNTupleUnpackerBase.h"

class RNTupleUnpackerRaw64 : public RNTupleUnpackerBase {
public:
  RNTupleUnpackerRaw64() {}
  ~RNTupleUnpackerRaw64() override {}

  unsigned long int unpack(const std::vector<std::string> &ins, const std::string &out) const override;

protected:
};

#endif