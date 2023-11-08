#ifndef puppiRawArrowSource_h
#define puppiRawArrowSource_h

#include "RArrowDS2.hxx"
#include <vector>
#include <fstream>
#include <cstdint>
#if defined(__GNUC__)
  #pragma GCC diagnostic push
  #pragma GCC diagnostic ignored "-Wshadow"
  #pragma GCC diagnostic ignored "-Wunused-parameter"
  #pragma GCC diagnostic ignored "-Wredundant-move"
#endif
#include <arrow/array.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/record_batch.h>
#if defined(__GNUC__)
  #pragma GCC diagnostic pop
#endif

class PuppiRawArrowSource : public ROOT::RDF::RArrowDS2::RecordBatchSource {
public:
  PuppiRawArrowSource(const std::string& flavour, const std::vector<std::string>& fileNames);
  virtual ~PuppiRawArrowSource();
  virtual std::shared_ptr<arrow::Schema> Schema() override { return schema_; }
  virtual void Start() override;
  virtual std::shared_ptr<arrow::RecordBatch> Next() override;

private:
  std::string flavour_;
  std::vector<std::string> fileNames_;
  unsigned int batchSize_;
  std::vector<std::fstream> files_;
  double unpackTime_;
  std::shared_ptr<arrow::Schema> schema_;
  // common stuff
  std::shared_ptr<arrow::DataType> puppiType_, puppisType_;
  std::shared_ptr<arrow::Field> runField_, orbitField_, bxField_, goodField_, puppisField_;
  std::vector<uint16_t> run_, bx_;
  std::vector<uint32_t> orbit_;
  std::shared_ptr<arrow::BooleanBuilder> goodBuilder_;
  std::vector<uint16_t> nwords_;
  std::vector<uint32_t> offsets_;
  // raw64 format
  std::vector<uint64_t> data_;
};
#endif