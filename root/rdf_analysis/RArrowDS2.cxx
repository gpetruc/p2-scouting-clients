// Author: Giovanni Petrucciani CERN
// Based on work from Giulio Eulisse CERN  2/2018

/*************************************************************************
 * Copyright (C) 1995-2018, Rene Brun and Fons Rademakers.               *
 * All rights reserved.                                                  *
 *                                                                       *
 * For the licensing terms see $ROOTSYS/LICENSE.                         *
 * For the list of contributors see $ROOTSYS/README/CREDITS.             *
 *************************************************************************/

// clang-format off
/** \class ROOT::RDF::RArrowDS2
    \ingroup dataframe
    \brief RDataFrame data source class to interface with Apache Arrow.

The RArrowDS2 implements a proxy RDataSource to be able to use Apache Arrow
tables with RDataFrame.

A RDataFrame that adapts an arrow::Table class can be constructed using the factory method
ROOT::RDF::FromArrow, which accepts one parameter:
1. An arrow::Table smart pointer.

The types of the columns are derived from the types in the associated
arrow::Schema.

*/
// clang-format on

#include <ROOT/RDF/Utils.hxx>
#include <RArrowDS2.hxx>
#include <snprintf.h>

#include <algorithm>
#include <memory>
#include <sstream>
#include <string>

#if defined(__GNUC__)
  #pragma GCC diagnostic push
  #pragma GCC diagnostic ignored "-Wshadow"
  #pragma GCC diagnostic ignored "-Wunused-parameter"
#endif
#include <arrow/array.h>
#include <arrow/table.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#if defined(__GNUC__)
  #pragma GCC diagnostic pop
#endif

namespace ROOT {
  namespace RDF {
    namespace Internal {

      class RArrowColumnReaderBase {
      protected:
        ROOT::RDF::RArrowDS2 *fSource;
        ROOT::RDF::RArrowDS2::ColumnAddress fAddr;
        ULong64_t fFirstEntry, fLastEntry;

      public:
        RArrowColumnReaderBase(ROOT::RDF::RArrowDS2 *source, const ROOT::RDF::RArrowDS2::ColumnAddress &addr)
            : fSource(source), fAddr(addr), fFirstEntry(0), fLastEntry(0) {}
        template <typename ArrowArrayType>
        inline bool maybeFetch(Long64_t entry, std::shared_ptr<ArrowArrayType> &arrayPtr) {
          if (ULong64_t(entry) >= fLastEntry) {
            fSource->GetRange(entry, fFirstEntry, fLastEntry);
            arrayPtr = std::static_pointer_cast<ArrowArrayType>(fSource->GetArrowColumn(fAddr));
            return true;
          } else {
            return false;
          }
        }
      };

      /// Helper class which keeps track for each slot where to get the entry.
      template <typename RootType, typename ArrowArrayType>
      class RArrowScalarReader : public ROOT::Detail::RDF::RColumnReaderBase, RArrowColumnReaderBase {
      private:
        std::shared_ptr<ArrowArrayType> fArray;
        Long64_t fEntry;
        RootType fCache;

      public:
        RArrowScalarReader(ROOT::RDF::RArrowDS2 *source, const ROOT::RDF::RArrowDS2::ColumnAddress &addr)
            : RArrowColumnReaderBase(source, addr), fEntry(std::numeric_limits<Long64_t>::max()) {}
        virtual ~RArrowScalarReader() {}
        void *GetImpl(Long64_t entry) override {
          if (entry != fEntry) {
            maybeFetch(entry, fArray);
            fCache = fArray->Value(entry - fFirstEntry);
            fEntry = entry;
          }
          return (void *)(&fCache);
        }
      };

      // specalization: zero-copy for native arrays other than bool and string
      template <typename RootType, typename T>
      class RArrowScalarReader<RootType, arrow::NumericArray<T>> : public ROOT::Detail::RDF::RColumnReaderBase,
                                                                   RArrowColumnReaderBase {
      private:
        std::shared_ptr<arrow::NumericArray<T>> fArray;

      public:
        RArrowScalarReader(ROOT::RDF::RArrowDS2 *source, const ROOT::RDF::RArrowDS2::ColumnAddress &addr)
            : RArrowColumnReaderBase(source, addr) {}
        virtual ~RArrowScalarReader() {}
        void *GetImpl(Long64_t entry) override {
          maybeFetch(entry, fArray);
          // need to cast due to 'long long' vs 'long' and 'signed char' vs 'char' differences
          const RootType *ptr = reinterpret_cast<const RootType *>(fArray->raw_values());
          return (void *)(ptr + (entry - fFirstEntry));
        }
      };

      template <typename ArrowOffsetsArrayType = arrow::UInt32Array>
      class RArrowOffsetsReader : public ROOT::Detail::RDF::RColumnReaderBase, RArrowColumnReaderBase {
      private:
        std::shared_ptr<ArrowOffsetsArrayType> fArray;
        Long64_t fEntry;
        UInt_t fCache;

      public:
        RArrowOffsetsReader(ROOT::RDF::RArrowDS2 *source, const ROOT::RDF::RArrowDS2::ColumnAddress &addr)
            : RArrowColumnReaderBase(source, addr), fEntry(std::numeric_limits<Long64_t>::max()) {}
        virtual ~RArrowOffsetsReader() {}
        void *GetImpl(Long64_t entry) override {
          if (entry != fEntry) {
            maybeFetch(entry, fArray);
            const auto *ptr = fArray->raw_values();
            Long64_t i = entry - fFirstEntry;
            fCache = ptr[i + 1] - ptr[i];
            fEntry = entry;
          }
          return (void *)(&fCache);
        }
      };

      template <typename T>
      class RArrowConstantReader : public ROOT::Detail::RDF::RColumnReaderBase {
      private:
        T fValue;

      public:
        RArrowConstantReader(const T &value) : fValue(value) {}
        virtual ~RArrowConstantReader() {}
        void *GetImpl(Long64_t) override { return (void *)(&fValue); }
      };

      // copy data from an Arrow Array to a ROOT RVec: generic implementation
      template <typename RootType, typename ArrowArrayType>
      struct RVecFiller {
        template <typename OffsType, typename LengthType>
        static void fill(ROOT::RVec<RootType> &cache, ArrowArrayType &array, OffsType offs, LengthType length) {
          cache.resize(length);
          for (LengthType i = 0; i < length; ++i, ++offs) {
            cache[i] = array.Value(offs);
          }
        }
      };
      // specalization: zero-copy for native arrays other than bool and string
      template <typename RootType, typename T>
      struct RVecFiller<RootType, arrow::NumericArray<T>> {
        template <typename OffsType, typename LengthType>
        static void fill(ROOT::RVec<RootType> &cache, arrow::NumericArray<T> &array, OffsType offs, LengthType length) {
          const RootType *ptr = reinterpret_cast<const RootType *>(array.raw_values() + offs);
          RVec<RootType> tmp(const_cast<RootType *>(ptr), length);
          std::swap(cache, tmp);
        }
      };

      template <typename RootType, typename ArrowArrayType, typename ArrowListType = arrow::ListArray>
      class RArrowListReader : public ROOT::Detail::RDF::RColumnReaderBase, RArrowColumnReaderBase {
      private:
        std::shared_ptr<ArrowListType> fListArray;
        std::shared_ptr<ArrowArrayType> fArray;
        Long64_t fEntry;
        ROOT::RVec<RootType> fCache;

      public:
        RArrowListReader(ROOT::RDF::RArrowDS2 *source, const ROOT::RDF::RArrowDS2::ColumnAddress &addr)
            : RArrowColumnReaderBase(source, addr), fEntry(std::numeric_limits<Long64_t>::max()) {}
        virtual ~RArrowListReader() {}
        template <typename T, typename O>
        void fillRVec(int64_t arrowEntry, O offs, O length) {}
        void *GetImpl(Long64_t entry) override {
          if (entry != fEntry) {
            if (maybeFetch(entry, fListArray))
              fArray = std::static_pointer_cast<ArrowArrayType>(fListArray->values());
            int64_t arrowEntry = entry - fFirstEntry;
            auto offs = fListArray->value_offset(arrowEntry);
            auto length = fListArray->value_offset(arrowEntry + 1) - offs;
            RVecFiller<RootType, ArrowArrayType>::fill(fCache, *fArray, offs, length);
            fEntry = entry;
          }
          return (void *)(&fCache);
        }
      };

      template <typename RootType, typename ArrowArrayType, typename ArrowInnerListType, typename ArrowOuterListType>
      class RArrowListReader2D : public ROOT::Detail::RDF::RColumnReaderBase, RArrowColumnReaderBase {
      private:
        std::shared_ptr<ArrowOuterListType> fOuterListArray;
        std::shared_ptr<ArrowInnerListType> fInnerListArray;
        std::shared_ptr<ArrowArrayType> fArray;
        Long64_t fEntry;
        ROOT::RVec<ROOT::RVec<RootType>> fCache;

      public:
        RArrowListReader2D(ROOT::RDF::RArrowDS2 *source, const ROOT::RDF::RArrowDS2::ColumnAddress &addr)
            : RArrowColumnReaderBase(source, addr), fEntry(std::numeric_limits<Long64_t>::max()) {}
        virtual ~RArrowListReader2D() {}
        template <typename T, typename O>
        void fillRVec(int64_t arrowEntry, O offs, O length) {}
        void *GetImpl(Long64_t entry) override {
          if (entry != fEntry) {
            if (maybeFetch(entry, fOuterListArray))
              fInnerListArray = std::static_pointer_cast<ArrowInnerListType>(fOuterListArray->values());
            fArray = std::static_pointer_cast<ArrowArrayType>(fInnerListArray->values());
            int64_t arrowEntry = entry - fFirstEntry;
            auto offs = fOuterListArray->value_offset(arrowEntry);
            auto length = fOuterListArray->value_offset(arrowEntry + 1) - offs;
            fCache.resize(length);
            std::cout << "Nested list <" << typeid(RootType).name() << "," << typeid(ArrowInnerListType).name() << ", "
                      << typeid(ArrowOuterListType).name() << " entry = " << entry << ", arrowEntry " << arrowEntry
                      << ", offs = " << offs << ", length = " << length << std::endl;
            for (auto i = 0; i < length; ++i) {
              auto offs2 = fInnerListArray->value_offset(offs);
              auto length2 = fInnerListArray->value_offset(++offs) - offs2;
              RVecFiller<RootType, ArrowArrayType>::fill(fCache[i], *fArray, offs2, length2);
              std::cout << "\t" << i << " (offs2  " << offs2 << ", length " << length2 << "): ";
              for (auto &v : fCache[i])
                std::cout << v << " ";
              std::cout << std::endl;
            }
            std::cout << "Final result: size =" << fCache.size() << std::endl;
            for (const auto &x : fCache) {
              std::cout << " - size = " << x.size() << ": ";
              for (auto &v : x)
                std::cout << v << " ";
              std::cout << std::endl;
            }
            fEntry = entry;
          }
          return (void *)(&fCache);
        }
      };
      template <typename RootType, typename ArrowArrayType, typename ArrowInnerListType>
      std::unique_ptr<ROOT::Detail::RDF::RColumnReaderBase> makeRArrowListReader2DInner(
          ROOT::RDF::RArrowDS2 *source,
          const ROOT::RDF::RArrowDS2::ColumnAddress &addr,
          const arrow::BaseListType &outer) {
        if (outer.id() == arrow::Type::LIST) {
          return std::make_unique<RArrowListReader2D<RootType, ArrowArrayType, ArrowInnerListType, arrow::ListArray>>(
              source, addr);
        } else if (outer.id() == arrow::Type::LARGE_LIST) {
          return std::make_unique<
              RArrowListReader2D<RootType, ArrowArrayType, ArrowInnerListType, arrow::LargeListArray>>(source, addr);
        } else if (outer.id() == arrow::Type::FIXED_SIZE_LIST) {
          return std::make_unique<
              RArrowListReader2D<RootType, ArrowArrayType, ArrowInnerListType, arrow::FixedSizeListArray>>(source,
                                                                                                           addr);
        } else {
          return std::unique_ptr<ROOT::Detail::RDF::RColumnReaderBase>();
        }
      }
      template <typename RootType, typename ArrowArrayType>
      std::unique_ptr<ROOT::Detail::RDF::RColumnReaderBase> makeRArrowListReader2D(
          ROOT::RDF::RArrowDS2 *source,
          const ROOT::RDF::RArrowDS2::ColumnAddress &addr,
          const arrow::BaseListType &inner,
          const arrow::BaseListType &outer) {
        if (inner.id() == arrow::Type::LIST) {
          return makeRArrowListReader2DInner<RootType, ArrowArrayType, arrow::ListArray>(source, addr, outer);
        } else if (inner.id() == arrow::Type::LARGE_LIST) {
          return makeRArrowListReader2DInner<RootType, ArrowArrayType, arrow::LargeListArray>(source, addr, outer);
        } else if (inner.id() == arrow::Type::FIXED_SIZE_LIST) {
          return makeRArrowListReader2DInner<RootType, ArrowArrayType, arrow::FixedSizeListArray>(source, addr, outer);
        } else {
          return std::unique_ptr<ROOT::Detail::RDF::RColumnReaderBase>();
        }
      }

      /// Helper to get the human readable name of type
      class RDFTypeNameGetter : public ::arrow::TypeVisitor {
      private:
        std::vector<std::string> fTypeName;

      public:
        arrow::Status Visit(const arrow::Int64Type &) override {
          fTypeName.push_back("Long64_t");
          return arrow::Status::OK();
        }
        arrow::Status Visit(const arrow::Int32Type &) override {
          fTypeName.push_back("Int_t");
          return arrow::Status::OK();
        }
        arrow::Status Visit(const arrow::Int16Type &) override {
          fTypeName.push_back("Short_t");
          return arrow::Status::OK();
        }
        arrow::Status Visit(const arrow::Int8Type &) override {
          fTypeName.push_back("Char_t");
          return arrow::Status::OK();
        }
        arrow::Status Visit(const arrow::UInt64Type &) override {
          fTypeName.push_back("ULong64_t");
          return arrow::Status::OK();
        }
        arrow::Status Visit(const arrow::UInt32Type &) override {
          fTypeName.push_back("UInt_t");
          return arrow::Status::OK();
        }
        arrow::Status Visit(const arrow::UInt16Type &) override {
          fTypeName.push_back("UShort_t");
          return arrow::Status::OK();
        }
        arrow::Status Visit(const arrow::UInt8Type &) override {
          fTypeName.push_back("UChar_t");
          return arrow::Status::OK();
        }
        arrow::Status Visit(const arrow::FloatType &) override {
          fTypeName.push_back("float");
          return arrow::Status::OK();
        }
        arrow::Status Visit(const arrow::DoubleType &) override {
          fTypeName.push_back("double");
          return arrow::Status::OK();
        }
        arrow::Status Visit(const arrow::StringType &) override {
          fTypeName.push_back("string");
          return arrow::Status::OK();
        }
        arrow::Status Visit(const arrow::BooleanType &) override {
          fTypeName.push_back("bool");
          return arrow::Status::OK();
        }
        arrow::Status Visit(const arrow::ListType &l) override {
          /// Recursively visit List types and map them to
          /// an RVec. We accumulate the result of the recursion on
          /// fTypeName so that we can create the actual type
          /// when the recursion is done.
          fTypeName.push_back("ROOT::VecOps::RVec<%s>");
          return l.value_type()->Accept(this);
        }
        arrow::Status Visit(const arrow::LargeListType &l) override {
          fTypeName.push_back("ROOT::VecOps::RVec<%s>");
          return l.value_type()->Accept(this);
        }
        arrow::Status Visit(const arrow::FixedSizeListType &l) override {
          fTypeName.push_back("ROOT::VecOps::RVec<%s>");
          return l.value_type()->Accept(this);
        }
        std::string result() {
          // This recursively builds a nested type.
          std::string result = "%s";
          char buffer[8192];
          for (size_t i = 0; i < fTypeName.size(); ++i) {
            snprintf(buffer, 8192, result.c_str(), fTypeName[i].c_str());
            result = buffer;
          }
          return result;
        }

        using ::arrow::TypeVisitor::Visit;
      };

      template <typename T>
      struct AlternativeSupportedType {
        typedef T type;
      };
      template <>
      struct AlternativeSupportedType<Long64_t> {
        typedef int64_t type;
      };
      template <>
      struct AlternativeSupportedType<ULong64_t> {
        typedef uint64_t type;
      };
      template <>
      struct AlternativeSupportedType<Char_t> {
        typedef int8_t type;
      };
      template <>
      struct AlternativeSupportedType<UChar_t> {
        typedef uint8_t type;
      };
      template<typename T>
      struct AlternativeSupportedType<ROOT::RVec<T>> {
        typedef typename AlternativeSupportedType<T>::type alternative_element_type;
        typedef ROOT::RVec<alternative_element_type> type;
      };
      /// Helper to make the necessary reader type.
      class ReaderMaker : public ::arrow::TypeVisitor {
      public:
        ReaderMaker(ROOT::RDF::RArrowDS2 *source,
                    const ROOT::RDF::RArrowDS2::ColumnAddress &addr,
                    const std::type_info &cppType)
            : fSource(source), fAddr(&addr), fCppType(&cppType), fLevel(0) {}
        virtual arrow::Status Visit(const arrow::Int64Type &) override { return make<Long64_t, arrow::Int64Array>(); }
        virtual arrow::Status Visit(const arrow::UInt64Type &) override {
          return make<ULong64_t, arrow::UInt64Array>();
        }
        virtual arrow::Status Visit(const arrow::Int32Type &) override { return make<Int_t, arrow::Int32Array>(); }
        virtual arrow::Status Visit(const arrow::UInt32Type &) override { return make<UInt_t, arrow::UInt32Array>(); }
        virtual arrow::Status Visit(const arrow::Int16Type &) override { return make<Short_t, arrow::Int16Array>(); }
        virtual arrow::Status Visit(const arrow::UInt16Type &) override { return make<UShort_t, arrow::UInt16Array>(); }
        virtual arrow::Status Visit(const arrow::Int8Type &) override { return make<Char_t, arrow::Int8Array>(); }
        virtual arrow::Status Visit(const arrow::UInt8Type &) override { return make<UChar_t, arrow::UInt8Array>(); }
        virtual arrow::Status Visit(const arrow::FloatType &) override { return make<Float_t, arrow::FloatArray>(); }
        virtual arrow::Status Visit(const arrow::DoubleType &) override { return make<Double_t, arrow::DoubleArray>(); }
        virtual arrow::Status Visit(const arrow::StringType &) override {
          return make<std::string, arrow::StringArray>();
        }
        virtual arrow::Status Visit(const arrow::BooleanType &) override { return make<Bool_t, arrow::BooleanArray>(); }
        virtual arrow::Status Visit(const arrow::ListType &t) override {
          fLevel++;
          fListStack.push_back(&t);
          return t.value_type()->Accept(this);
        }
        virtual arrow::Status Visit(const arrow::LargeListType &t) override {
          fLevel++;
          fListStack.push_back(&t);
          return t.value_type()->Accept(this);
        }
        virtual arrow::Status Visit(const arrow::FixedSizeListType &t) override {
          fLevel++;
          fListStack.push_back(&t);
          return t.value_type()->Accept(this);
        }
        using ::arrow::TypeVisitor::Visit;

        std::unique_ptr<ROOT::Detail::RDF::RColumnReaderBase> result() { return std::move(fReader); }

      private:
        ROOT::RDF::RArrowDS2 *fSource;
        const ROOT::RDF::RArrowDS2::ColumnAddress *fAddr;
        const std::type_info *fCppType;
        std::unique_ptr<ROOT::Detail::RDF::RColumnReaderBase> fReader;
        std::vector<const arrow::BaseListType *> fListStack;
        unsigned int fLevel = 0;

        template <typename RT, typename AT>
        arrow::Status make() {
          if (fLevel == 0) {
            if (*fCppType != typeid(RT) && *fCppType != typeid(typename AlternativeSupportedType<RT>::type)) {
              return arrow::Status::TypeError(std::string("Bad type for scalar, found ") + typeid(RT).name() +
                                              ", requested " + fCppType->name());
            }
            fReader = std::make_unique<RArrowScalarReader<RT, AT>>(fSource, *fAddr);
            return arrow::Status::OK();
          } else if (fLevel == 1) {
            if (*fCppType != typeid(ROOT::RVec<RT>) && *fCppType != typeid(typename AlternativeSupportedType<ROOT::RVec<RT>>::type)) {
              return arrow::Status::TypeError(std::string("Bad type for 1D vector, found ") +
                                              typeid(ROOT::RVec<RT>).name() + ", requested " + fCppType->name());
            }
            if (fListStack.front()->id() == arrow::Type::LIST) {
              fReader = std::make_unique<RArrowListReader<RT, AT>>(fSource, *fAddr);
            } else if (fListStack.front()->id() == arrow::Type::LARGE_LIST) {
              fReader = std::make_unique<RArrowListReader<RT, AT, arrow::LargeListArray>>(fSource, *fAddr);
            } else if (fListStack.front()->id() == arrow::Type::FIXED_SIZE_LIST) {
              fReader = std::make_unique<RArrowListReader<RT, AT, arrow::FixedSizeListArray>>(fSource, *fAddr);
            } else {
              return arrow::Status::NotImplemented("");
            }
            return arrow::Status::OK();
          } else if (fLevel == 2) {
            if (*fCppType != typeid(ROOT::RVec<ROOT::RVec<RT>>) && *fCppType != typeid(typename AlternativeSupportedType<ROOT::RVec<ROOT::RVec<RT>>>::type)) {
              return arrow::Status::TypeError(std::string("Bad type for 2D vector, found ") +
                                              typeid(ROOT::RVec<ROOT::RVec<RT>>).name() + ", requested " +
                                              fCppType->name());
            }
            fReader = makeRArrowListReader2D<RT, AT>(fSource, *fAddr, *fListStack.front(), *fListStack.back());
            if (fReader) {
              return arrow::Status::OK();
            } else {
              return arrow::Status::NotImplemented("");
            }
          } else {
            return arrow::Status::NotImplemented("");
          }
        }
      };

      class IPCStreamSource : public RArrowDS2::RecordBatchSource {
      public:
        IPCStreamSource(const std::string &fileName) {
          auto ok = arrow::io::ReadableFile::Open(fileName);
          if (!ok.ok())
            throw std::runtime_error("Can't open " + fileName);
          file_ = *ok;
          openFile();
          firstRecord_ = true;
          schema_ = reader_->schema();
        }
        ~IPCStreamSource() override {}
        std::shared_ptr<arrow::Schema> Schema() override { return schema_; }
        void Start() override {
          if (!firstRecord_)
            openFile();
        }
        std::shared_ptr<arrow::RecordBatch> Next() override {
          firstRecord_ = false;
          auto ret = reader_->Next();
          if (!ret.ok())
            throw std::runtime_error("Failure reading from file");
          return *ret;
        }

      private:
        std::shared_ptr<arrow::io::ReadableFile> file_;
        std::shared_ptr<arrow::ipc::RecordBatchStreamReader> reader_;
        std::shared_ptr<arrow::Schema> schema_;
        bool firstRecord_;
        void openFile() {
          auto ok = arrow::ipc::RecordBatchStreamReader::Open(file_);
          if (!ok.ok())
            throw std::runtime_error("Failure opening file");
          reader_ = *ok;
        }
      };

      class IPCFileSource : public RArrowDS2::RecordBatchSource {
      public:
        IPCFileSource(const std::string &fileName) {
          auto ok = arrow::io::ReadableFile::Open(fileName);
          //auto ok = arrow::io::MemoryMappedFile::Create(fileName, std::filesystem::file_size(fileName));
          if (!ok.ok())
            throw std::runtime_error("Can't open " + fileName);
          file_ = *ok;
          auto ok2 = arrow::ipc::RecordBatchFileReader::Open(file_);
          reader_ = *ok2;
          schema_ = reader_->schema();
          lastBatch_ = reader_->num_record_batches();
          iBatch_ = 0;
        }
        ~IPCFileSource() override {}
        std::shared_ptr<arrow::Schema> Schema() override { return schema_; }
        void Start() override { iBatch_ = 0; }
        std::shared_ptr<arrow::RecordBatch> Next() override {
          if (iBatch_ == lastBatch_) {
            return std::shared_ptr<arrow::RecordBatch>();
          }
          auto ret = reader_->ReadRecordBatch(iBatch_);
          if (!ret.ok())
            throw std::runtime_error("Failure reading from file");
          iBatch_++;
          return *ret;
        }

      private:
        std::shared_ptr<arrow::io::ReadableFile> file_;
        //std::shared_ptr<arrow::io::MemoryMappedFile> file_;
        std::shared_ptr<arrow::ipc::RecordBatchFileReader> reader_;
        std::shared_ptr<arrow::Schema> schema_;
        unsigned int iBatch_, lastBatch_;
      };

      class ArrowTableSource : public RArrowDS2::RecordBatchSource {
      public:
        ArrowTableSource(std::shared_ptr<arrow::Table> table) : table_(table) {}
        ~ArrowTableSource() override {}
        std::shared_ptr<arrow::Schema> Schema() override { return table_->schema(); }
        void Start() override { reader_ = std::make_unique<arrow::TableBatchReader>(table_); }
        std::shared_ptr<arrow::RecordBatch> Next() override { return *reader_->Next(); }

      private:
        std::shared_ptr<arrow::Table> table_;
        std::unique_ptr<arrow::TableBatchReader> reader_;
      };
    }  // namespace Internal

    ////////////////////////////////////////////////////////////////////////
    /// Constructor to create an Arrow RDataSource for RDataFrame.
    /// \param[in] inTable the arrow Table to observe.
    /// \param[in] inColumns the name of the columns to use
    /// In case columns is empty, we use all the columns found in the table
    RArrowDS2::RArrowDS2(std::unique_ptr<RecordBatchSource> src, std::vector<std::string> const &requestedColumns)
        : fSource(std::move(src)),
          fSchema(fSource->Schema()),
          fRecordBatch(),
          fFirstEntryOfBatch(0),
          fLastEntryOfBatch(0),
          fNSlots(0) {
      ColumnAddress addr(1);
      for (int n = fSchema->num_fields(); addr[0] < n; ++addr[0]) {
        auto &field = fSchema->field(addr[0]);
        maybeAddColumns(field->name(), *field->type(), addr, requestedColumns);
      }
    }

    void RArrowDS2::maybeAddColumns(const std::string &name,
                                    const arrow::DataType &type,
                                    ColumnAddress const &base,
                                    std::vector<std::string> const &requestedColumns) {
      const auto tid = type.id();
      if (tid == arrow::Type::STRUCT) {
        // unpack data members
        const arrow::StructType *structType = dynamic_cast<const arrow::StructType *>(&type);
        ColumnAddress subaddr{base};  // make copy
        subaddr.push_back(0);
        for (unsigned int i = 0, n = structType->num_fields(); i < n; ++i) {
          auto &subfield = structType->field(i);
          subaddr.back() = i;
          maybeAddColumns(name + "." + subfield->name(), *subfield->type(), subaddr, requestedColumns);
        }
      } else if (tid == arrow::Type::LIST || tid == arrow::Type::LARGE_LIST || tid == arrow::Type::FIXED_SIZE_LIST) {
        const arrow::BaseListType *listType = dynamic_cast<const arrow::BaseListType *>(&type);
        ColumnAddress subaddr{base};  // make copy
        subaddr.push_back(0);
        // Add the contents
        maybeAddColumns(name, *listType->value_type(), subaddr, requestedColumns);
        // then the size column
        subaddr.back() = 1;
        std::string countName = "R_rdf_sizeof_" + name;
        if (requestedColumns.empty() ||
            (std::find(requestedColumns.begin(), requestedColumns.end(), countName) != requestedColumns.end())) {
          fColumnNames.push_back(countName);
          fColumnAddresses[countName] = base;
          //std::cout << "Created " << countName << " with address " ;
          //for (auto  & i : base) std::cout << i << ".";
          //std::cout << std::endl;
        }
      } else if ((tid == arrow::Type::STRING) || (dynamic_cast<const arrow::PrimitiveCType *>(&type) != nullptr)) {
        if (requestedColumns.empty() ||
            (std::find(requestedColumns.begin(), requestedColumns.end(), name) != requestedColumns.end())) {
          fColumnNames.push_back(name);
          fColumnAddresses[name] = base;
          //std::cout << "Created " << name << " with type " << type.name() << " address " ;
          //for (auto  & i : base) std::cout << i << ".";
          //std::cout << std::endl;
        }
      } else {
        if (requestedColumns.empty() ||
            (std::find(requestedColumns.begin(), requestedColumns.end(), name) != requestedColumns.end())) {
          std::cout << "Ignoring column " << name << " of type " << type.ToString() << ", not supported";
        }
      }
    }

    ////////////////////////////////////////////////////////////////////////
    /// Destructor.
    RArrowDS2::~RArrowDS2() {}

    std::shared_ptr<arrow::DataType> RArrowDS2::GetArrowType(const std::string &name) const {
      auto addr = fColumnAddresses.find(std::string(name));
      if (addr == fColumnAddresses.end()) {
        return std::shared_ptr<arrow::DataType>();
      }
      return GetArrowType(addr->second);
    }

    std::shared_ptr<arrow::DataType> RArrowDS2::GetArrowType(const ColumnAddress &addr) const {
      auto type = fSchema->field(addr.front())->type();
      std::vector<int> listSize;  // fize for fixed lists, -32 or -64 for long or short lists
      for (unsigned int i = 1, n = addr.size(); i < n; ++i) {
        if (type->id() == arrow::Type::LIST) {
          if (addr[i] == 0) {
            listSize.push_back(-32);
            type = static_cast<const arrow::ListType &>(*type).value_type();
          } else {
            type = arrow::uint32();
          }
        } else if (type->id() == arrow::Type::LARGE_LIST) {
          if (addr[i] == 0) {
            listSize.push_back(-64);
            type = static_cast<const arrow::LargeListType &>(*type).value_type();
          } else {
            type = arrow::uint64();
          }
        } else if (type->id() == arrow::Type::FIXED_SIZE_LIST) {
          auto listType = static_cast<const arrow::FixedSizeListType *>(type.get());
          if (addr[i] == 0) {
            listSize.push_back(listType->list_size());
            type = listType->value_type();
          } else {
            type = arrow::uint32();
          }
        } else if (type->id() == arrow::Type::STRUCT) {
          type = static_cast<const arrow::StructType &>(*type).field(addr[i])->type();
        } else {
          std::string msg = "called for column ";
          msg += fSchema->field(addr.front())->name();
          msg += " unpacking at i = " + std::to_string(i) + "/" + std::to_string(n);
          msg += " current type " + type->ToString() + ", id " + std::to_string(type->id());
          throw std::runtime_error("RArrowDS2::GetArrowType ERROR: " + msg);
        }
      }
      while (!listSize.empty()) {
        if (listSize.back() == -32)
          type = arrow::list(type);
        else if (listSize.back() == -64)
          type = arrow::large_list(type);
        else
          type = arrow::fixed_size_list(type, listSize.back());
        listSize.pop_back();
      }
      return type;
    }

    std::shared_ptr<arrow::Array> RArrowDS2::GetArrowColumn(const std::string &name) {
      auto addr = fColumnAddresses.find(std::string(name));
      if (addr == fColumnAddresses.end()) {
        return std::shared_ptr<arrow::Array>();
      }
      return GetArrowColumn(addr->second);
    }

    std::shared_ptr<arrow::Array> RArrowDS2::GetArrowColumn(const ColumnAddress &addr) {
      auto ret = fRecordBatch->column(addr.front());
      std::vector<std::shared_ptr<arrow::Array>> offsetArrays;
      std::vector<int> sizeArrays;  // size for fixed lists, -32 or -64 for short or long lists
      for (unsigned int i = 1, n = addr.size(); i < n; ++i) {
        auto id = ret->type()->id();
        if (id == arrow::Type::LIST) {
          const arrow::ListArray *list = static_cast<const arrow::ListArray *>(ret.get());
          if (addr[i] == 0) {
            offsetArrays.push_back(list->offsets());
            sizeArrays.push_back(-32);
            ret = list->values();
          } else {
            ret = list->offsets();
          }
        } else if (id == arrow::Type::LARGE_LIST) {
          const arrow::LargeListArray *list = static_cast<const arrow::LargeListArray *>(ret.get());
          if (addr[i] == 0) {
            offsetArrays.push_back(list->offsets());
            sizeArrays.push_back(-64);
            ret = list->values();
          } else {
            ret = list->offsets();
          }
        } else if (id == arrow::Type::FIXED_SIZE_LIST) {
          const arrow::FixedSizeListArray *list = static_cast<const arrow::FixedSizeListArray *>(ret.get());
          if (addr[i] == 0) {
            offsetArrays.emplace_back();
            sizeArrays.push_back(list->list_type()->list_size());
            ret = list->values();
          } else {
            ret = std::shared_ptr<arrow::Array>();
          }
        } else if (id == arrow::Type::STRUCT) {
          ret = *static_cast<const arrow::StructArray &>(*ret).GetFlattenedField(addr[i]);
        } else {
          std::string msg = "called for column ";
          msg += fSchema->field(addr.front())->name();
          msg += " unpacking at i = " + std::to_string(i) + "/" + std::to_string(n);
          msg += " current type " + ret->type()->ToString() + ", id " + std::to_string(id);
          throw std::runtime_error("RArrowDS2::GetArrowColumn ERROR: " + msg);
        }
      }
      while (!offsetArrays.empty()) {
        if (sizeArrays.back() == -32) {
          ret = *arrow::ListArray::FromArrays(*offsetArrays.back(), *ret);
        } else if (sizeArrays.back() == -64) {
          ret = *arrow::LargeListArray::FromArrays(*offsetArrays.back(), *ret);
        } else {
          ret = *arrow::FixedSizeListArray::FromArrays(ret, sizeArrays.back());
        }
        offsetArrays.pop_back();
        sizeArrays.pop_back();
      }
      return ret;
    }

    const std::vector<std::string> &RArrowDS2::GetColumnNames() const { return fColumnNames; }

    std::vector<std::pair<ULong64_t, ULong64_t>> RArrowDS2::GetEntryRanges() {
      std::vector<std::pair<ULong64_t, ULong64_t>> ret;
      fRecordBatch = fSource->Next();
      if (!fRecordBatch)
        return ret;
      fFirstEntryOfBatch = fLastEntryOfBatch;
      fLastEntryOfBatch += fRecordBatch->num_rows();
      ULong64_t stride = fRecordBatch->num_rows() / fNSlots;
      ULong64_t start = fFirstEntryOfBatch;
      for (unsigned int i = 0; i < fNSlots; ++i, start += stride) {
        ret.emplace_back(start, start + stride);
      }
      ret.back().second = fLastEntryOfBatch;
      return ret;
    }

    std::string RArrowDS2::GetTypeName(std::string_view colName) const {
      auto type = GetArrowType(std::string(colName));
      if (!type) {
        std::string msg = "The dataset does not have column ";
        msg += colName;
        throw std::runtime_error(msg);
      }
      if (colName.substr(0, 13) == "R_rdf_sizeof_") {
        return "UInt_t";  // all sizes we return are uint32_t
      }
      Internal::RDFTypeNameGetter typeGetter;
      auto status = type->Accept(&typeGetter);
      if (status.ok() == false) {
        std::string msg = "RArrowDS2 does not support column ";
        msg += colName;
        msg += " of type " + type->name();
        throw std::runtime_error(msg);
      }
      return typeGetter.result();
    }

    void RArrowDS2::GetRange(Long64_t entry, ULong64_t &first, ULong64_t &last) {
      assert(fFirstEntryOfBatch <= ULong64_t(entry) && ULong64_t(entry) < fLastEntryOfBatch);
      first = fFirstEntryOfBatch;
      last = fLastEntryOfBatch;
    }

    bool RArrowDS2::HasColumn(std::string_view colName) const {
      return (fColumnAddresses.find(std::string(colName)) != fColumnAddresses.end());
    }

    bool RArrowDS2::SetEntry(unsigned int /*slot*/, ULong64_t /*entry*/) { return true; }

    void RArrowDS2::InitSlot(unsigned int /*slot*/, ULong64_t /*entry*/) {}

    void RArrowDS2::SetNSlots(unsigned int nSlots) {
      assert(0U == fNSlots && "Setting the number of slots even if the number of slots is different from zero.");
      fNSlots = nSlots;
    }

    std::unique_ptr<ROOT::Detail::RDF::RColumnReaderBase> RArrowDS2::GetColumnReaders(unsigned int /*slot*/,
                                                                                      std::string_view columnName,
                                                                                      const std::type_info &cppType) {
      std::unique_ptr<ROOT::Detail::RDF::RColumnReaderBase> ret;
      auto addr = fColumnAddresses.find(std::string(columnName));
      assert(addr != fColumnAddresses.end());
      auto type = GetArrowType(addr->second);
      if (type) {
        if (columnName.substr(0, 13) == "R_rdf_sizeof_") {
          if (cppType != typeid(uint32_t)) {
            throw std::runtime_error("RArrowDS2 column " + std::string(columnName) + " arrow type " + type->ToString() +
                                     " should be read as uint32_t, while " + std::string(cppType.name()) +
                                     " was requested.");
          } else if (type->id() == arrow::Type::LIST) {
            return std::make_unique<Internal::RArrowOffsetsReader<arrow::UInt32Array>>(this, addr->second);
          } else if (type->id() == arrow::Type::LARGE_LIST) {
            return std::make_unique<Internal::RArrowOffsetsReader<arrow::UInt64Array>>(this, addr->second);
          } else if (type->id() == arrow::Type::FIXED_SIZE_LIST) {
            return std::make_unique<Internal::RArrowConstantReader<UInt_t>>(
                static_cast<arrow::FixedSizeListType &>(*type).list_size());
          } else {
            throw std::runtime_error("RArrowDS2 can't make a size column for " + std::string(columnName) +
                                     " arrow type " + type->ToString());
          }
        } else {
          Internal::ReaderMaker maker(this, addr->second, cppType);
          auto ok = type->Accept(&maker);
          if (ok.ok()) {
            return maker.result();
          } else {
            throw std::runtime_error("RArrowDS2 can't make a maker for " + std::string(columnName) + " arrow type " +
                                     type->ToString() + ": " + ": " + ok.message());
          }
        }
      } else {
        throw std::runtime_error("RArrowDS2 can't make a type for " + std::string(columnName) + " arrow type " +
                                 type->ToString());
      }
    }

    void RArrowDS2::Initialize() { fSource->Start(); }

    std::string RArrowDS2::GetLabel() { return "RArrowDS2"; }

    /// \brief Factory method to create a Apache Arrow RDataFrame.
    ///
    /// Creates a RDataFrame using an arrow::Table as input.
    /// \param[in] table an apache::arrow table to use as a source / to observe.
    /// \param[in] columnNames the name of the columns to use
    /// In case columnNames is empty, we use all the columns found in the table
    RDataFrame FromArrowIPCStream(const std::string &fileName, std::vector<std::string> const &columnNames) {
      std::unique_ptr<RArrowDS2::RecordBatchSource> src = std::make_unique<Internal::IPCStreamSource>(fileName);
      ROOT::RDataFrame rdf(std::make_unique<RArrowDS2>(std::move(src), columnNames));
      return rdf;
    }
    RDataFrame FromArrowIPCFile(const std::string &fileName, std::vector<std::string> const &columnNames) {
      std::unique_ptr<RArrowDS2::RecordBatchSource> src = std::make_unique<Internal::IPCFileSource>(fileName);
      ROOT::RDataFrame rdf(std::make_unique<RArrowDS2>(std::move(src), columnNames));
      return rdf;
    }
    RDataFrame FromArrowTable(std::shared_ptr<arrow::Table> table, std::vector<std::string> const &columnNames) {
      std::unique_ptr<RArrowDS2::RecordBatchSource> src = std::make_unique<Internal::ArrowTableSource>(table);
      ROOT::RDataFrame rdf(std::make_unique<RArrowDS2>(std::move(src), columnNames));
      return rdf;
    }
  }  // namespace RDF

}  // namespace ROOT
