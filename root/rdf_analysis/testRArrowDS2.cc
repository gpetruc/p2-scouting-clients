#include <cmath>
#include <ROOT/RDataFrame.hxx>
#include <ROOT/RVec.hxx>
#include <Math/Vector4D.h>
#include <Math/GenVector/LorentzVector.h>
#include <Math/GenVector/PtEtaPhiM4D.h>
#include "ROOT/RNTupleDS.hxx"
#include <ROOT/RSnapshotOptions.hxx>
#include "RArrowDS2.hxx"
#include <chrono>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/array.h>
#include <arrow/array/builder_primitive.h>

void analyze(ROOT::RDataFrame &d, unsigned long int &ntot, unsigned long int &npass, const std::string &outFile) {
  d.Describe().Print();
  auto c0 = d.Count();
  auto d1 = d.Filter(
                 [](bool x, double y, const ROOT::VecOps::RVec<float> &z) {
                   //printf("x = %d, y = %g, z = %g, %g, %g, ...\n", int(x), std::sin(y/7.0), std::cos(z[0]), std::sin(z[1]), z[2]);
                   return x && (std::sin(y / 7.0) > 0.7) && std::abs(std::cos(z.front())) < 0.3;
                 },
                 {"boolVar", "doubleVar", "Puppi.floatMem"})
                .Alias("nPuppi", "#Puppi")
                .Alias("Puppi_boolMem", "Puppi.boolMem")
                .Alias("Puppi_floatMem", "Puppi.floatMem")
                .Alias("Puppi_intMem", "Puppi.intMem")
                .Alias("Puppi_uint64Mem", "Puppi.uint64Mem");
  auto c1 = d1.Count();
  ROOT::RDF::RSnapshotOptions opts;
  opts.fCompressionLevel = 0;
  std::vector<std::string> columns = {"boolVar",
                                      "int8Var",
                                      "uint8Var",
                                      "int16Var",
                                      "uint16Var",
                                      "uintVar",
                                      "int64Var",
                                      "doubleVar",
                                      "nPuppi",
                                      "Puppi_boolMem",
                                      "Puppi_floatMem",
                                      "Puppi_intMem",
                                      "Puppi_uint64Mem"};
  d1.Snapshot<bool,
              Char_t,
              uint8_t,
              int16_t,
              uint16_t,
              uint32_t,
              Long64_t,
              double,
              uint32_t,
              ROOT::RVec<bool>,
              ROOT::RVec<float>,
              ROOT::RVec<int32_t>,
              ROOT::RVec<uint64_t>>("Events", outFile.c_str(), columns, opts);
  ntot = *c0;
  npass = *c1;
}

int writeFile(const std::string &format, const std::string &fOut) {
  std::shared_ptr<arrow::Field> doubleVarField = arrow::field("doubleVar", arrow::float64());
  std::shared_ptr<arrow::Field> int8VarField = arrow::field("int8Var", arrow::int8());
  std::shared_ptr<arrow::Field> uint8VarField = arrow::field("uint8Var", arrow::uint8());
  std::shared_ptr<arrow::Field> int16VarField = arrow::field("int16Var", arrow::int16());
  std::shared_ptr<arrow::Field> uint16VarField = arrow::field("uint16Var", arrow::uint16());
  std::shared_ptr<arrow::Field> uintVarField = arrow::field("uintVar", arrow::uint32());
  std::shared_ptr<arrow::Field> int64VarField = arrow::field("int64Var", arrow::int64());
  std::shared_ptr<arrow::Field> boolVarField = arrow::field("boolVar", arrow::boolean());

  std::shared_ptr<arrow::Field> floatMemField = arrow::field("floatMem", arrow::float32());
  std::shared_ptr<arrow::Field> intMemField = arrow::field("intMem", arrow::int32());
  std::shared_ptr<arrow::Field> uint64MemField = arrow::field("uint64Mem", arrow::uint64());
  std::shared_ptr<arrow::Field> boolMemField = arrow::field("boolMem", arrow::boolean());

  std::shared_ptr<arrow::DataType> puppiType =
      arrow::struct_({floatMemField, intMemField, uint64MemField, boolMemField});

  std::shared_ptr<arrow::DataType> puppisType;
  if (format.find("list64") != std::string::npos)
    puppisType = arrow::large_list(puppiType);
  else if (format.find("fixlist") != std::string::npos)
    puppisType = arrow::fixed_size_list(puppiType, 7);
  else
    puppisType = arrow::list(puppiType);

  std::shared_ptr<arrow::Field> puppiField = arrow::field("Puppi", puppisType);
  std::shared_ptr<arrow::Schema> schema = arrow::schema({doubleVarField,
                                                         int8VarField,
                                                         uint8VarField,
                                                         int16VarField,
                                                         uint16VarField,
                                                         uintVarField,
                                                         int64VarField,
                                                         boolVarField,
                                                         puppiField});

  arrow::DoubleBuilder doubleVarBuilder;
  arrow::Int8Builder int8VarBuilder;
  arrow::UInt8Builder uint8VarBuilder;
  arrow::Int16Builder int16VarBuilder;
  arrow::UInt16Builder uint16VarBuilder;
  arrow::UInt32Builder uintVarBuilder;
  arrow::Int64Builder int64VarBuilder;
  arrow::BooleanBuilder boolVarBuilder;

  arrow::FloatBuilder floatMemBuilder;
  arrow::Int32Builder intMemBuilder;
  arrow::UInt64Builder uint64MemBuilder;
  arrow::BooleanBuilder boolMemBuilder;
  std::vector<uint32_t> offsets(1, 0);
  std::vector<uint64_t> offsets64;
  std::shared_ptr<arrow::io::FileOutputStream> outputFile = *arrow::io::FileOutputStream::Open(fOut);
  arrow::ipc::IpcWriteOptions ipcWriteOptions = arrow::ipc::IpcWriteOptions::Defaults();
  std::shared_ptr<arrow::ipc::RecordBatchWriter> batchWriter;
  if (format.find("ipc_stream") == 0) {
    batchWriter = *arrow::ipc::MakeStreamWriter(outputFile, schema, ipcWriteOptions);
  } else if (format.find("ipc_file") == 0) {
    batchWriter = *arrow::ipc::MakeFileWriter(outputFile, schema, ipcWriteOptions);
  }
  for (int ibatch = 0, nev = 35; ibatch < 4; ++ibatch) {
    for (int iev = 0; iev < nev; ++iev) {
      for (int ipuppi = 0; ipuppi < 7; ++ipuppi) {
        floatMemBuilder.Append((1000 * ibatch + 100 * iev + ipuppi) * 0.1);
        intMemBuilder.Append(1000 * ibatch + 100 * iev + ipuppi);
        uint64MemBuilder.Append(1000 * ibatch + 100 * iev + ipuppi);
        boolMemBuilder.Append(ipuppi % 2 == 0 || iev % 3 == 0);
      }
      offsets.emplace_back(offsets.back() + 7);
      doubleVarBuilder.Append(1000 * ibatch + iev);
      int8VarBuilder.Append(10 * ibatch + iev);
      uint8VarBuilder.Append(10 * ibatch + iev);
      int16VarBuilder.Append(100 * ibatch + iev);
      uint16VarBuilder.Append(100 * ibatch + iev);
      int64VarBuilder.Append(10000 * ibatch + iev * 100);
      uintVarBuilder.Append(10000 * ibatch + iev * 100);
      boolVarBuilder.Append(true);
    }
    auto floatMemArray = floatMemBuilder.Finish();
    auto intMemArray = intMemBuilder.Finish();
    auto uint64MemArray = uint64MemBuilder.Finish();
    auto boolMemArray = boolMemBuilder.Finish();
    auto doubleVarArray = doubleVarBuilder.Finish();
    auto int8VarArray = int8VarBuilder.Finish();
    auto uint8VarArray = uint8VarBuilder.Finish();
    auto int16VarArray = int16VarBuilder.Finish();
    auto uint16VarArray = uint16VarBuilder.Finish();
    auto int64VarArray = int64VarBuilder.Finish();
    auto uintVarArray = uintVarBuilder.Finish();
    auto boolVarArray = boolVarBuilder.Finish();

    std::shared_ptr<arrow::Array> flatPuppiArray(new arrow::StructArray(
        puppiType, offsets.back(), {*floatMemArray, *intMemArray, *uint64MemArray, *boolMemArray}));
    std::shared_ptr<arrow::Array> puppiArray;
    if (format.find("list64") != std::string::npos) {
      offsets64.reserve(offsets.size());
      for (auto o : offsets)
        offsets64.emplace_back(o);
      auto largePuppi =
          std::make_shared<arrow::LargeListArray>(puppisType, nev, arrow::Buffer::Wrap(offsets64), flatPuppiArray);
      puppiArray = std::static_pointer_cast<arrow::Array>(largePuppi);
    } else if (format.find("fixlist") != std::string::npos) {
      puppiArray = *arrow::FixedSizeListArray::FromArrays(flatPuppiArray, 7);
    } else {
      auto listPuppi =
          std::make_shared<arrow::ListArray>(puppisType, nev, arrow::Buffer::Wrap(offsets), flatPuppiArray);
      puppiArray = std::static_pointer_cast<arrow::Array>(listPuppi);
    }
    std::shared_ptr<arrow::RecordBatch> batch = arrow::RecordBatch::Make(schema,
                                                                         nev,
                                                                         {*doubleVarArray,
                                                                          *int8VarArray,
                                                                          *uint8VarArray,
                                                                          *int16VarArray,
                                                                          *uint16VarArray,
                                                                          *uintVarArray,
                                                                          *int64VarArray,
                                                                          *boolVarArray,
                                                                          puppiArray});
    auto validres = batch->ValidateFull();
    if (!validres.ok()) {
      std::cout << "Validate: " << validres.ToString() << std::endl;
      return 7;
    }
    if (batchWriter)
      if (!batchWriter->WriteRecordBatch(*batch).ok())
        return 2;
    offsets.resize(1);
    offsets64.clear();
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
    printf("              %s write format fileOut\nformat := ipc_(file|stream)[_list64|_fixlist]\n", argv[0]);
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
  auto verbosity = ROOT::Experimental::RLogScopedVerbosity(
      ROOT::Detail::RDF::RDFLogChannel(),
      verbose ? ROOT::Experimental::ELogLevel::kInfo : ROOT::Experimental::ELogLevel::kWarning);
  // and suppress RNTuple verbosity
  auto rntVerbosity =
      ROOT::Experimental::RLogScopedVerbosity(ROOT::Experimental::NTupleLog(), ROOT::Experimental::ELogLevel::kError);

  unsigned long int ntot, npass;
  if (format.find("ipc_file") == 0) {
    ROOT::RDataFrame d = ROOT::RDF::FromArrowIPCFile(fileIn, {});
    analyze(d, ntot, npass, fileOut);
  } else if (format.find("ipc_stream") == 0) {
    ROOT::RDataFrame d = ROOT::RDF::FromArrowIPCStream(fileIn, {});
    analyze(d, ntot, npass, fileOut);
  }

  double dt = (std::chrono::duration<double>(std::chrono::steady_clock::now() - tstart)).count();
  printf("Run on file %s, %lu events, selected %lu events (%.4f), time %.3fs\n",
         fileIn.c_str(),
         ntot,
         npass,
         npass / float(ntot),
         dt);

  return 0;
}
