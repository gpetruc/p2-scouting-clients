#ifndef p2_clients_apache_IPCUnpackerRaw64_h
#define p2_clients_apache_IPCUnpackerRaw64_h
#include "IPCUnpackerBase.h"

class IPCUnpackerRaw64 : public IPCUnpackerBase {
public:
  IPCUnpackerRaw64(unsigned int batchsize);
  ~IPCUnpackerRaw64() {}
  void unpackAndCommitBatch();

protected:
  std::shared_ptr<arrow::DataType> puppisType_;
  std::shared_ptr<arrow::Field> puppiField_;
  std::vector<int> offsets_;
};

#endif