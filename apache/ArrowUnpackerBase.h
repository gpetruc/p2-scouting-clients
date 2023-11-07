#ifndef p2_clients_apache_ArrowUnpackerBase_h
#define p2_clients_apache_ArrowUnpackerBase_h
#include "../UnpackerBase.h"
#include "ApacheUnpackMaker.h"
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/array.h>
#include <arrow/array/builder_primitive.h>
#ifdef USE_PARQUET
  #include <parquet/arrow/writer.h>
#endif

class ArrowUnpackerBase : public UnpackerBase {
public:
  ArrowUnpackerBase(unsigned int batchsize, ApacheUnpackMaker::Spec::FileKind fileKind);
  ~ArrowUnpackerBase() override {}
  Report myUnpackFiles(const std::vector<std::string> &ins, const std::string &out);
  void setThreads(unsigned int threads) override;
  void setCompression(const std::string &algo, unsigned int level) override {
    compressionMethod_ = algo;
    compressionLevel_ = level;
  }
  void bookOutput(const std::string &out) override final;
  unsigned long int closeOutput() override final;

  virtual void unpackAndCommitBatch() = 0;

  void fillBase(uint16_t run, uint32_t orbit, uint16_t bx, bool good) {
    run_[entriesInBatch_] = run;
    orbit_[entriesInBatch_] = orbit;
    bx_[entriesInBatch_] = bx;
    goodBuilder_->Append(good);
  }

  void fillEvent(uint16_t run, uint32_t orbit, uint16_t bx, bool good, uint16_t nwords, const uint64_t *words) override {
    fillBase(run, orbit, bx, good);
    nwords_[entriesInBatch_] = nwords;
    data_.insert(data_.end(), words, words + nwords);
    entriesInBatch_++;
    if (entriesInBatch_ == batchsize_)
      unpackAndCommitBatch();
  }

  void writeRecordBatch(const arrow::RecordBatch &batch) {
    if (batchWriter_)
      batchWriter_->WriteRecordBatch(batch);
#ifdef USE_PARQUET
    else if (parquetWriter_)
      parquetWriter_->WriteRecordBatch(batch);
#endif
  }

protected:
  unsigned int batchsize_;
  ApacheUnpackMaker::Spec::FileKind fileKind_;
  std::string compressionMethod_;
  int compressionLevel_;
  std::string fout_;
  std::shared_ptr<arrow::Schema> schema_;
  std::shared_ptr<arrow::io::FileOutputStream> outputFile_;
  // for IPC files
  std::shared_ptr<arrow::ipc::RecordBatchWriter> batchWriter_;
#ifdef USE_PARQUET
  // for Parquet files
  std::shared_ptr<parquet::arrow::FileWriter> parquetWriter_;
#endif

  //
  unsigned long int entriesInBatch_, batches_;
  std::shared_ptr<arrow::Field> runField_, orbitField_, bxField_, goodField_;
  std::vector<uint16_t> run_, bx_;
  std::vector<uint32_t> orbit_;
  std::shared_ptr<arrow::BooleanBuilder> goodBuilder_;
  std::vector<uint16_t> nwords_;
  std::vector<uint64_t> data_;
};

#endif