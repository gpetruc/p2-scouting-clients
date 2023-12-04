#include <cmath>
#include <ROOT/RDataFrame.hxx>
#include <ROOT/RVec.hxx>
#include <Math/Vector4D.h>
#include <Math/GenVector/LorentzVector.h>
#include <Math/GenVector/PtEtaPhiM4D.h>
#include "ROOT/RNTupleDS.hxx"
#include <ROOT/RSnapshotOptions.hxx>
#include <chrono>
#if defined(__GNUC__)
  #pragma GCC diagnostic push
  #pragma GCC diagnostic ignored "-Wshadow"
  #pragma GCC diagnostic ignored "-Wredundant-move"
  #pragma GCC diagnostic ignored "-Wunused-parameter"
#endif
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/array.h>
#include <arrow/array/builder_primitive.h>
#if defined(__GNUC__)
  #pragma GCC diagnostic pop
#endif
#include "RArrowDS2.hxx"

/* Demonstrate writing and reading a file in Arrow format testing the following
* All integer data types 8, 16, 32, 64 bits, signed and unsigned
* Floats and doubles
* booleans and lists of bools
* Structures, nested
* lists, large lists, fixed - size lists, both of primitive types and of structures
* 2D lists (fixed or variable)
*/

template <typename T>
T gen_val(int event, int entry = 0) {
  return 37 * event + 11 * entry;
}
template <>
bool gen_val<bool>(int event, int entry) {
  return entry % 3 == 1 || event % 5 == 0;
}
template <>
double gen_val<double>(int event, int entry) {
  return 0.7 * M_PI * event - 4.414 * entry;
}
template <>
float gen_val<float>(int event, int entry) {
  return gen_val<double>(event, entry);
}
template <>
int64_t gen_val<int64_t>(int event, int entry) {
  return gen_val<int>(event, entry) * 1000;
}
unsigned int gen_few(int event, int offs = 0) { return (event % 3) + 5 + offs; }

template <typename T>
ROOT::RDF::RNode checkSingle(ROOT::RDF::RNode &d, const std::string &name) {
  return d.Filter(
      [name](T obsVal, ULong64_t ievent) {
        if (ievent == 0)
          std::cout << "Checking 1D " << name << " " << typeid(T).name() << std::endl;
        T expVal = gen_val<T>(ievent);
        if (obsVal != expVal) {
          std::cout << "Mismatch for field " << name << ", entry " << ievent << ", found " << obsVal << ", expected "
                    << expVal << std::endl;
          return false;
        } else {
          return true;
        }
      },
      {name, "rdfentry_"});
};
template <typename T>
ROOT::RDF::RNode checkList(ROOT::RDF::RNode &d, const std::string &name, int noffs = 0, int eoffs = 0) {
  return d.Filter(
      [name, noffs, eoffs](const ROOT::RVec<T> &obsVal, ULong64_t ievent) {
        if (ievent == 0)
          std::cout << "Checking 1D " << name << " " << typeid(ROOT::RVec<T>).name() << std::endl;
        unsigned int obsSize = obsVal.size(), expSize = noffs < 0 ? -noffs : gen_few(ievent, noffs);
        if (obsSize != expSize) {
          std::cout << "Mismatch for field " << name << ", entry " << ievent << ", size " << obsSize << ", expected "
                    << expSize << std::endl;
          return false;
        }
        for (unsigned int i = 0; i < obsSize; ++i) {
          T expVal = gen_val<T>(ievent, i + eoffs);
          if (obsVal[i] != expVal) {
            std::cout << "Mismatch for field " << name << ", entry " << ievent << ", at index " << i << ", found "
                      << obsVal[i] << ", expected " << expVal << std::endl;
            return false;
          }
        }
        return true;
      },
      {name, "rdfentry_"});
};
template <typename T>
ROOT::RDF::RNode checkList2D(ROOT::RDF::RNode &d, const std::string &name, int noffs, int noffs2) {
  return d.Filter(
      [name, noffs, noffs2](const ROOT::RVec<ROOT::RVec<T>> &obsVal, ULong64_t ievent) {
        if (ievent == 0)
          std::cout << "Checking 2D " << name << " " << typeid(ROOT::RVec<ROOT::RVec<T>>).name() << std::endl;
        unsigned int obsSize = obsVal.size(), expSize = noffs < 0 ? -noffs : gen_few(ievent, noffs);
        static int maxErrs = 10;
        if (obsSize != expSize) {
          if (--maxErrs > 0)
            std::cout << "Mismatch for field " << name << ", entry " << ievent << ", outer size " << obsSize
                      << ", expected " << expSize << std::endl;
          return false;
        }
        for (unsigned int i = 0, j0 = 0; i < obsSize; ++i) {
          const auto &obsVal2 = obsVal[i];
          unsigned int obsSize2 = obsVal2.size();
          unsigned int expSize2 = noffs2 < 0 ? -noffs2 : gen_few(ievent + i);
          if (obsSize2 != expSize2) {
            if (--maxErrs > 0)
              std::cout << "Mismatch for field " << name << ", entry " << ievent << ", outer index " << i
                        << ", inner size " << obsSize2 << ", expected " << expSize2 << std::endl;
            return false;
          }
          for (unsigned int j = 0; j < obsSize2; ++j, ++j0) {
            unsigned int expentry = (noffs < 0 || noffs2 < 0) ? j0 : i + j;
            T expVal = gen_val<T>(ievent, expentry);
            if (obsVal2[j] != expVal) {
              if (--maxErrs > 0)
                std::cout << "Mismatch for field " << name << ", entry " << ievent << ", at index " << i << ", " << j
                          << ", found " << obsVal2[j] << ", expected " << expVal << " (at " << expentry << ")"
                          << std::endl;
              return false;
            }
          }
        }
        return true;
      },
      {name, "rdfentry_"});
};

void analyze(ROOT::RDataFrame &d,
             unsigned long int &ntot,
             unsigned long int &ncheck,
             unsigned long int &npass,
             const std::string &outFile) {
  d.Describe().Print();
  auto c0 = d.Count();
  // then we vaildate all entries
  ROOT::RDF::RNode dv = d;
  dv = checkSingle<double>(dv, "doubleVar");
  dv = checkSingle<Char_t>(dv, "int8Var");
  dv = checkSingle<uint8_t>(dv, "uint8Var");
  dv = checkSingle<int16_t>(dv, "int16Var");
  dv = checkSingle<UInt_t>(dv, "uintVar");
  dv = checkSingle<int64_t>(dv, "int64Var");
  dv = checkSingle<bool>(dv, "boolVar");
  dv = checkSingle<uint16_t>(dv, "GlobalStruct.uint16");
  dv = checkList<float>(dv, "GlobalStruct.floats", 2, 5);
  dv = checkList<uint16_t>(dv, "uint16FixList", -2, 0);
  dv = checkList<bool>(dv, "boolFixList", -5, 10);
  dv = checkList<Char_t>(dv, "int8List64", 10, 2);
  dv = checkList<float>(dv, "Struct.floatMem");
  dv = checkList<int>(dv, "Struct.intMem");
  dv = checkList<uint64_t>(dv, "Struct.SubStruct.uint64SubMem");
  dv = checkList<bool>(dv, "Struct.SubStruct.boolSubMem");
  dv = checkList2D<float>(dv, "floats3x4", -3, -4);
  dv = checkList2D<int>(dv, "Struct.intsMem", 0, 0);
  dv = checkList2D<bool>(dv, "Struct.SubStruct.boolsSubMem", 0, -4);
  auto cv = dv.Count();
  auto d1 = d.Filter(
                 [](bool x, double y, const ROOT::VecOps::RVec<float> &z) {
                   return x && (std::sin(y / 7.0) > 0.17) &&
                          (std::abs(std::cos(z.front())) < 0.3 || std::abs(std::cos(z.back())) < 0.3);
                 },
                 {"boolVar", "doubleVar", "Struct.floatMem"})
                .Alias("nint8List64", "#int8List64")
                .Alias("GlobalStruct_nfloats", "#GlobalStruct.floats")
                .Alias("GlobalStruct_uint16", "GlobalStruct.uint16")
                .Alias("GlobalStruct_floats", "GlobalStruct.floats")
                .Alias("nStruct", "#Struct")
                .Alias("Struct_floatMem", "Struct.floatMem")
                .Alias("Struct_intMem", "Struct.intMem")
                .Alias("Struct_SubStruct_uint64SubMem", "Struct.SubStruct.uint64SubMem")
                .Alias("Struct_SubStruct_boolSubMem", "Struct.SubStruct.boolSubMem");

  auto c1 = d1.Count();
  ROOT::RDF::RSnapshotOptions opts;
  opts.fCompressionLevel = 0;
  std::vector<std::string> columns = {"doubleVar",
                                      "int8Var",
                                      "uint8Var",
                                      "int16Var",
                                      "uintVar",
                                      "int64Var",
                                      "boolVar",
                                      "uint16FixList",
                                      "boolFixList",
                                      "nint8List64",
                                      "int8List64",
                                      "GlobalStruct_uint16",
                                      "GlobalStruct_nfloats",
                                      "GlobalStruct_floats",
                                      "nStruct",
                                      "Struct_floatMem",
                                      "Struct_intMem",
                                      "Struct_SubStruct_uint64SubMem",
                                      "Struct_SubStruct_boolSubMem"};
  d1.Snapshot<double,
              Char_t,
              uint8_t,
              int16_t,
              UInt_t,
              Long64_t,
              bool,
              // lists
              ROOT::RVec<uint16_t>,
              ROOT::RVec<bool>,
              uint32_t,
              ROOT::RVec<Char_t>,
              // global struct
              uint16_t,
              uint32_t,
              ROOT::RVec<float>,
              // struct + substruct
              uint32_t,
              ROOT::RVec<float>,
              ROOT::RVec<int32_t>,
              ROOT::RVec<uint64_t>,
              ROOT::RVec<bool>>("Events", outFile.c_str(), columns, opts);
  ntot = *c0;
  ncheck = *cv;
  npass = *c1;
}

int writeFile(const std::string &format, const std::string &fOut) {
  std::shared_ptr<arrow::Field> doubleVarField = arrow::field("doubleVar", arrow::float64());
  std::shared_ptr<arrow::Field> int8VarField = arrow::field("int8Var", arrow::int8());
  std::shared_ptr<arrow::Field> uint8VarField = arrow::field("uint8Var", arrow::uint8());
  std::shared_ptr<arrow::Field> int16VarField = arrow::field("int16Var", arrow::int16());
  std::shared_ptr<arrow::Field> uintVarField = arrow::field("uintVar", arrow::uint32());
  std::shared_ptr<arrow::Field> int64VarField = arrow::field("int64Var", arrow::int64());
  std::shared_ptr<arrow::Field> boolVarField = arrow::field("boolVar", arrow::boolean());

  std::shared_ptr<arrow::Field> uint16FixListField =
      arrow::field("uint16FixList", arrow::fixed_size_list(arrow::uint16(), 2));
  std::shared_ptr<arrow::Field> boolFixListField =
      arrow::field("boolFixList", arrow::fixed_size_list(arrow::boolean(), 5));

  std::shared_ptr<arrow::Field> int8List64Field = arrow::field("int8List64", arrow::large_list(arrow::int8()));

  std::shared_ptr<arrow::Field> floats3x4Field =
      arrow::field("floats3x4", arrow::fixed_size_list(arrow::fixed_size_list(arrow::float32(), 4), 3));

  std::shared_ptr<arrow::Field> uint16GMemField = arrow::field("uint16", arrow::uint16());
  std::shared_ptr<arrow::Field> floatsGMemField = arrow::field("floats", arrow::list(arrow::float32()));
  std::shared_ptr<arrow::DataType> globalStructType = arrow::struct_({uint16GMemField, floatsGMemField});
  std::shared_ptr<arrow::Field> globalStructField = arrow::field("GlobalStruct", globalStructType);

  std::shared_ptr<arrow::Field> floatMemField = arrow::field("floatMem", arrow::float32());
  std::shared_ptr<arrow::Field> intMemField = arrow::field("intMem", arrow::int32());
  std::shared_ptr<arrow::Field> intsMemField = arrow::field("intsMem", arrow::list(arrow::int32()));
  std::shared_ptr<arrow::Field> uint64SubMemField = arrow::field("uint64SubMem", arrow::uint64());
  std::shared_ptr<arrow::Field> boolSubMemField = arrow::field("boolSubMem", arrow::boolean());
  std::shared_ptr<arrow::Field> boolsSubMemField =
      arrow::field("boolsSubMem", arrow::fixed_size_list(arrow::boolean(), 4));
  std::shared_ptr<arrow::DataType> substructType =
      arrow::struct_({uint64SubMemField, boolSubMemField, boolsSubMemField});
  std::shared_ptr<arrow::Field> substructField = arrow::field("SubStruct", substructType);

  std::shared_ptr<arrow::DataType> structType =
      arrow::struct_({floatMemField, intMemField, intsMemField, substructField});
  std::shared_ptr<arrow::DataType> structsType = arrow::list(structType);
  ;
  std::shared_ptr<arrow::Field> structField = arrow::field("Struct", structsType);

  std::shared_ptr<arrow::Schema> schema = arrow::schema({doubleVarField,
                                                         int8VarField,
                                                         uint8VarField,
                                                         int16VarField,
                                                         uintVarField,
                                                         int64VarField,
                                                         boolVarField,
                                                         uint16FixListField,
                                                         boolFixListField,
                                                         int8List64Field,
                                                         floats3x4Field,
                                                         globalStructField,
                                                         structField});

  arrow::DoubleBuilder doubleVarBuilder;
  arrow::Int8Builder int8VarBuilder;
  arrow::UInt8Builder uint8VarBuilder;
  arrow::Int16Builder int16VarBuilder;
  arrow::UInt32Builder uintVarBuilder;
  arrow::Int64Builder int64VarBuilder;
  arrow::BooleanBuilder boolVarBuilder;

  arrow::UInt16Builder uint16FixListBuilder;
  arrow::BooleanBuilder boolFixListBuilder;

  arrow::Int8Builder int8List64Builder;
  std::vector<uint64_t> offsets64(1, 0);

  arrow::FloatBuilder floats3x4Builder;

  arrow::UInt16Builder uint16GMemBuilder;
  arrow::FloatBuilder floatsGMemBuilder;
  std::vector<uint32_t> goffsets(1, 0);

  arrow::FloatBuilder floatMemBuilder;
  arrow::Int32Builder intMemBuilder, intsMemBuilder;
  std::vector<uint32_t> intOffsets(1, 0);
  arrow::UInt64Builder uint64SubMemBuilder;
  arrow::BooleanBuilder boolSubMemBuilder, boolsSubMemBuilder;
  std::vector<uint32_t> offsets(1, 0);
  std::shared_ptr<arrow::io::FileOutputStream> outputFile = *arrow::io::FileOutputStream::Open(fOut);
  arrow::ipc::IpcWriteOptions ipcWriteOptions = arrow::ipc::IpcWriteOptions::Defaults();
  std::shared_ptr<arrow::ipc::RecordBatchWriter> batchWriter;
  if (format.find("ipc_stream") == 0) {
    batchWriter = *arrow::ipc::MakeStreamWriter(outputFile, schema, ipcWriteOptions);
  } else if (format.find("ipc_file") == 0) {
    batchWriter = *arrow::ipc::MakeFileWriter(outputFile, schema, ipcWriteOptions);
  }
  for (int ibatch = 0, ievent = 0, nev = 250; ibatch < 8; ++ibatch) {
    for (int ie = 0; ie < nev; ++ie, ++ievent) {
      doubleVarBuilder.Append(gen_val<double>(ievent));
      int8VarBuilder.Append(gen_val<int8_t>(ievent));
      uint8VarBuilder.Append(gen_val<uint8_t>(ievent));
      int16VarBuilder.Append(gen_val<int16_t>(ievent));
      uintVarBuilder.Append(gen_val<uint32_t>(ievent));
      int64VarBuilder.Append(gen_val<int64_t>(ievent));
      boolVarBuilder.Append(gen_val<bool>(ievent));
      for (unsigned int ientry = 0; ientry < 2; ++ientry)
        uint16FixListBuilder.Append(gen_val<uint32_t>(ievent, ientry));
      for (unsigned int ientry = 0; ientry < 5; ++ientry)
        boolFixListBuilder.Append(gen_val<bool>(ievent, ientry + 10));

      unsigned int nint8 = gen_few(ievent, 10);
      for (unsigned int ientry = 0; ientry < nint8; ++ientry)
        int8List64Builder.Append(gen_val<int8_t>(ievent, ientry + 2));
      offsets64.push_back(offsets64.back() + nint8);

      for (unsigned int ientry = 0, i = 0; i < 3; ++i)
        for (unsigned int j = 0; j < 4; ++j, ++ientry)
          floats3x4Builder.Append(gen_val<float>(ievent, ientry));

      uint16GMemBuilder.Append(gen_val<uint16_t>(ievent));
      unsigned int ngfloat = gen_few(ievent, 2);
      for (unsigned int ientry = 0; ientry < ngfloat; ++ientry)
        floatsGMemBuilder.Append(gen_val<float>(ievent, ientry + 5));
      goffsets.push_back(goffsets.back() + ngfloat);

      unsigned int nstruct = gen_few(ievent);
      for (unsigned int ientry = 0; ientry < nstruct; ++ientry) {
        floatMemBuilder.Append(gen_val<float>(ievent, ientry));
        intMemBuilder.Append(gen_val<int>(ievent, ientry));
        unsigned int nsubints = gen_few(ievent + ientry);
        for (unsigned int j = 0; j < nsubints; ++j)
          intsMemBuilder.Append(gen_val<int>(ievent, ientry + j));
        intOffsets.push_back(intOffsets.back() + nsubints);
        uint64SubMemBuilder.Append(gen_val<uint64_t>(ievent, ientry));
        boolSubMemBuilder.Append(gen_val<bool>(ievent, ientry));
        for (unsigned int j = 0; j < 4; ++j) {
          boolsSubMemBuilder.Append(gen_val<bool>(ievent, ientry * 4 + j));
        }
      }
      offsets.emplace_back(offsets.back() + nstruct);
    }

    // then build things, bottom-up
    auto uint64SubMemArray = *uint64SubMemBuilder.Finish();
    auto boolSubMemArray = *boolSubMemBuilder.Finish();
    auto flatBoolsSubMemArray = *boolsSubMemBuilder.Finish();
    std::shared_ptr<arrow::Array> boolsSubMemArray = *arrow::FixedSizeListArray::FromArrays(flatBoolsSubMemArray, 4);
    std::shared_ptr<arrow::Array> flatSubStructArray(
        new arrow::StructArray(substructType, offsets.back(), {uint64SubMemArray, boolSubMemArray, boolsSubMemArray}));

    auto floatMemArray = *floatMemBuilder.Finish();
    auto intMemArray = *intMemBuilder.Finish();
    auto flatIntsMemArray = *intsMemBuilder.Finish();
    std::shared_ptr<arrow::Array> intsMemArray(new arrow::ListArray(
        arrow::list(arrow::int32()), offsets.back(), arrow::Buffer::Wrap(intOffsets), flatIntsMemArray));
    std::shared_ptr<arrow::Array> flatStructArray(new arrow::StructArray(
        structType, offsets.back(), {floatMemArray, intMemArray, intsMemArray, flatSubStructArray}));
    std::shared_ptr<arrow::Array> structArray(
        new arrow::ListArray(structsType, nev, arrow::Buffer::Wrap(offsets), flatStructArray));

    auto flatFloatsGMemArray = *floatsGMemBuilder.Finish();
    std::shared_ptr<arrow::Array> floatsGMemArray(
        new arrow::ListArray(arrow::list(arrow::float32()), nev, arrow::Buffer::Wrap(goffsets), flatFloatsGMemArray));
    auto uint16GMemArray = *uint16GMemBuilder.Finish();
    std::shared_ptr<arrow::Array> globalStructArray(
        new arrow::StructArray(globalStructType, nev, {uint16GMemArray, floatsGMemArray}));

    auto doubleVarArray = *doubleVarBuilder.Finish();
    auto int8VarArray = *int8VarBuilder.Finish();
    auto uint8VarArray = *uint8VarBuilder.Finish();
    auto int16VarArray = *int16VarBuilder.Finish();
    auto uintVarArray = *uintVarBuilder.Finish();
    auto int64VarArray = *int64VarBuilder.Finish();
    auto boolVarArray = *boolVarBuilder.Finish();

    auto flatUint16FixListArray = *uint16FixListBuilder.Finish();
    auto flatBoolFixListArray = *boolFixListBuilder.Finish();
    std::shared_ptr<arrow::Array> uint16FixListArray, boolFixListArray;
    uint16FixListArray = *arrow::FixedSizeListArray::FromArrays(flatUint16FixListArray, 2);
    boolFixListArray = *arrow::FixedSizeListArray::FromArrays(flatBoolFixListArray, 5);

    auto floats3x4Array0 = *floats3x4Builder.Finish();
    std::shared_ptr<arrow::Array> floats3x4Array1, floats3x4Array;
    floats3x4Array1 = *arrow::FixedSizeListArray::FromArrays(floats3x4Array0, 4);
    floats3x4Array = *arrow::FixedSizeListArray::FromArrays(floats3x4Array1, 3);

    auto flatInt8List64Array = *int8List64Builder.Finish();
    std::shared_ptr<arrow::Array> int8List64Array(new arrow::LargeListArray(
        arrow::large_list(arrow::int8()), nev, arrow::Buffer::Wrap(offsets64), flatInt8List64Array));

    std::shared_ptr<arrow::RecordBatch> batch = arrow::RecordBatch::Make(schema,
                                                                         nev,
                                                                         {doubleVarArray,
                                                                          int8VarArray,
                                                                          uint8VarArray,
                                                                          int16VarArray,
                                                                          uintVarArray,
                                                                          int64VarArray,
                                                                          boolVarArray,
                                                                          uint16FixListArray,
                                                                          boolFixListArray,
                                                                          int8List64Array,
                                                                          floats3x4Array,
                                                                          globalStructArray,
                                                                          structArray});
    auto validres = batch->ValidateFull();
    if (!validres.ok()) {
      std::cout << "Validate: " << validres.ToString() << std::endl;
      return 7;
    }
    if (batchWriter)
      if (!batchWriter->WriteRecordBatch(*batch).ok())
        return 2;
    offsets.resize(1);
    goffsets.resize(1);
    offsets64.resize(1);
    intOffsets.resize(1);
  }  // end of batch
  if (!batchWriter->Close().ok())
    return 3;
  if (!outputFile->Close().ok())
    return 4;
  printf("Wrote to %s in format %s\n", fOut.c_str(), format.c_str());
  return 0;
}
int main(int argc, char **argv) {
  if (argc < 4) {
    printf("Error: usage: %s format fileIn fileOut\nformat := ipc_file ipc_stream\n", argv[0]);
    printf("              %s write format fileOut\nformat := ipc_(file|stream)\n", argv[0]);
    return 1;
  }
  std::string op = argv[1];
  if (op == "write") {
    return writeFile(argv[2], argv[3]);
  }
  std::string format = argv[1];
  std::string fileIn = argv[2];
  std::string fileOut = argv[3];
  bool verbose = true;
  auto tstart = std::chrono::steady_clock::now();
  //increase verbosity to see how long this is taking
  auto verbosity = ROOT::Experimental::RLogScopedVerbosity(ROOT::Detail::RDF::RDFLogChannel(),
                                                           ROOT::Experimental::ELogLevel::kWarning);
  // and suppress RNTuple verbosity
  auto rntVerbosity =
      ROOT::Experimental::RLogScopedVerbosity(ROOT::Experimental::NTupleLog(), ROOT::Experimental::ELogLevel::kError);

  unsigned long int ntot, ncheck, npass;
  if (format.find("ipc_file") == 0) {
    ROOT::RDataFrame d = ROOT::RDF::FromArrowIPCFile(fileIn, {});
    analyze(d, ntot, ncheck, npass, fileOut);
  } else if (format.find("ipc_stream") == 0) {
    ROOT::RDataFrame d = ROOT::RDF::FromArrowIPCStream(fileIn, {});
    analyze(d, ntot, ncheck, npass, fileOut);
  } else {
    std::cout << " Unsupported format " << format << std::endl;
    return 1;
  }

  double dt = (std::chrono::duration<double>(std::chrono::steady_clock::now() - tstart)).count();
  printf("Run on file %s, %lu events, validated %lu events (%.4f), selected %lu events (%.4f), time %.3fs\n",
         fileIn.c_str(),
         ntot,
         ncheck,
         ncheck / float(ntot),
         npass,
         npass / float(ntot),
         dt);

  return ntot > 0 && ncheck == ntot && npass > 0 && npass < ntot ? 0 : 7;
}
