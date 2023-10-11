/*************************************************************************
 * Copyright (C) 1995-2021, Rene Brun and Fons Rademakers.               *
 * All rights reserved.                                                  *
 *                                                                       *
 * For the licensing terms see $ROOTSYS/LICENSE.                         *
 * For the list of contributors see $ROOTSYS/README/CREDITS.             *
 *************************************************************************/

#ifndef ROOT_RARROWTDS2
#define ROOT_RARROWTDS2

#include "ROOT/RDataFrame.hxx"
#include "ROOT/RDataSource.hxx"

#include <memory>
#include <vector>
#include <unordered_map>

namespace arrow {
  class RecordBatch;
  class Schema;
  class DataType;
  class Array;
}  // namespace arrow

namespace ROOT {
  namespace RDF {

    class RArrowDS2 final : public RDataSource {
    public:
      class RecordBatchSource {
      public:
        RecordBatchSource() {}
        virtual ~RecordBatchSource() {}
        virtual std::shared_ptr<arrow::Schema> Schema() = 0;
        virtual void Start() = 0;
        virtual std::shared_ptr<arrow::RecordBatch> Next() = 0;
      };

      RArrowDS2(std::unique_ptr<RecordBatchSource> src, std::vector<std::string> const &requestedColumns);
      ~RArrowDS2();
      const std::vector<std::string> &GetColumnNames() const final;
      std::vector<std::pair<ULong64_t, ULong64_t>> GetEntryRanges() final;
      std::string GetTypeName(std::string_view colName) const final;
      bool HasColumn(std::string_view colName) const final;
      bool SetEntry(unsigned int slot, ULong64_t entry) final;
      void InitSlot(unsigned int slot, ULong64_t firstEntry) final;
      void SetNSlots(unsigned int nSlots) final;
      void Initialize() final;
      std::string GetLabel() final;

      typedef std::vector<int> ColumnAddress;
      std::shared_ptr<arrow::DataType> GetArrowType(const std::string &name) const;
      std::shared_ptr<arrow::DataType> GetArrowType(const ColumnAddress &addrs) const;
      std::shared_ptr<arrow::Array> GetArrowColumn(const std::string &name);
      std::shared_ptr<arrow::Array> GetArrowColumn(const ColumnAddress &addr);
      void GetRange(Long64_t entry, ULong64_t &first, ULong64_t &last);

    private:
      std::unique_ptr<RecordBatchSource> fSource;
      std::shared_ptr<arrow::Schema> fSchema;
      std::shared_ptr<arrow::RecordBatch> fRecordBatch;
      ULong64_t fFirstEntryOfBatch, fLastEntryOfBatch;

      size_t fNSlots = 0U;

      std::vector<std::string> fColumnNames;
      std::unordered_map<std::string, ColumnAddress> fColumnAddresses;

      virtual Record_t GetColumnReadersImpl(std::string_view /*name*/, const std::type_info &) override {
        return Record_t();
      }
      virtual std::unique_ptr<ROOT::Detail::RDF::RColumnReaderBase> GetColumnReaders(unsigned int,
                                                                                     std::string_view,
                                                                                     const std::type_info &) override;

      void maybeAddColumns(const std::string &name,
                           const arrow::DataType &field,
                           ColumnAddress const &base,
                           std::vector<std::string> const &requestedColumns);
    };

    RDataFrame FromArrowIPCStream(const std::string &fileName, std::vector<std::string> const &columnNames);
    RDataFrame FromArrowIPCFile(const std::string &fileName, std::vector<std::string> const &columnNames);

  }  // namespace RDF

}  // namespace ROOT

#endif
