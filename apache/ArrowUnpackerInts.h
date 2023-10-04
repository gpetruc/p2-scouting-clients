#ifndef p2_clients_apache_ArrowUnpackerInts_h
#define p2_clients_apache_ArrowUnpackerInts_h
#include "ArrowUnpackerBase.h"

class ArrowUnpackerInts : public ArrowUnpackerBase {
public:
  ArrowUnpackerInts(unsigned int batchsize, ApacheUnpackMaker::Spec::FileKind fileKind);
  ~ArrowUnpackerInts() {}
  void unpackAndCommitBatch();

protected:
  std::shared_ptr<arrow::Field> ptField_, etaField_, phiField_, z0Field_, dxyField_, wpuppiField_;
  std::shared_ptr<arrow::Field> pidField_, qualityField_;
  std::shared_ptr<arrow::DataType> puppiType_, puppisType_;
  std::shared_ptr<arrow::Field> puppiField_;
  std::vector<uint16_t> pt_, wpuppi_;
  std::vector<int16_t> eta_, phi_, z0_;
  std::vector<int8_t> dxy_;
  std::vector<uint8_t> pid_;
  std::vector<uint8_t> quality_;
  std::vector<int> offsets_;
};

#endif