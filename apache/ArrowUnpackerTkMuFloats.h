#ifndef p2_clients_apache_ArrowUnpackerTkMuFloats_h
#define p2_clients_apache_ArrowUnpackerTkMuFloats_h
#include "ArrowUnpackerBase.h"

class ArrowUnpackerTkMuFloats : public ArrowUnpackerBase {
public:
  ArrowUnpackerTkMuFloats(unsigned int batchsize, ApacheUnpackMaker::Spec::FileKind fileKind, bool float16 = false);
  ~ArrowUnpackerTkMuFloats() {}
  void unpackAndCommitBatch();

protected:
  bool float16_;
  std::shared_ptr<arrow::DataType> floatType_;
  std::shared_ptr<arrow::Field> ptField_, etaField_, phiField_, z0Field_, d0Field_, betaField_;
  std::shared_ptr<arrow::Field> chargeField_, qualityField_, isolationField_;
  std::shared_ptr<arrow::DataType> tkmuType_, tkmusType_;
  std::shared_ptr<arrow::Field> tkmuField_;
  std::vector<float> pt_, eta_, phi_, z0_, d0_, beta_;
  std::vector<int8_t> charge_;
  std::vector<uint8_t> quality_, isolation_;
  std::vector<int> offsets_;
};

#endif