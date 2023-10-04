#ifndef p2_clients_apache_ArrowUnpackerRaw64_h
#define p2_clients_apache_ArrowUnpackerRaw64_h
#include "ArrowUnpackerBase.h"

class ArrowUnpackerRaw64 : public ArrowUnpackerBase {
public:
  ArrowUnpackerRaw64(unsigned int batchsize, ApacheUnpackMaker::Spec::FileKind fileKind);
  ~ArrowUnpackerRaw64() {}
  void unpackAndCommitBatch();

protected:
  std::shared_ptr<arrow::DataType> puppisType_;
  std::shared_ptr<arrow::Field> puppiField_;
  std::vector<int> offsets_;
};

#endif