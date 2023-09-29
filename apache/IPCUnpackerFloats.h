#ifndef p2_clients_apache_IPCUnpackerFloats_h
#define p2_clients_apache_IPCUnpackerFloats_h
#include "IPCUnpackerBase.h"

class IPCUnpackerFloats : public IPCUnpackerBase {
public:
  IPCUnpackerFloats(unsigned int batchsize, bool float16 = false);
  ~IPCUnpackerFloats() {}
  void unpackAndCommitBatch();

protected:
  bool float16_;
  std::shared_ptr<arrow::DataType> floatType_;
  std::shared_ptr<arrow::Field> ptField_, etaField_, phiField_, z0Field_, dxyField_, wpuppiField_;
  std::shared_ptr<arrow::Field> pdgidField_, qualityField_;
  std::shared_ptr<arrow::DataType> puppiType_, puppisType_;
  std::shared_ptr<arrow::Field> puppiField_;
  std::vector<float> pt_, eta_, phi_, z0_, dxy_, wpuppi_;
  std::vector<int16_t> pdgid_;
  std::vector<uint8_t> quality_;
  std::vector<int> offsets_;
};

#endif