#include <arrow/array.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/array/builder_nested.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/pretty_print.h>
#include <arrow/status.h>
#include <arrow/result.h>
#include <arrow/table.h>

#include <iostream>
#include <cstdio>
#include <chrono>
#include <string>

#include <getopt.h>

#include "../root/unpack.h"

template <typename FltType, typename FltBuilderType = typename arrow::TypeTraits<FltType>::BuilderType>
void run_ipc(std::fstream &fin,
             unsigned long &entries,
             unsigned long &nbatches,
             unsigned long batchsize,
             std::shared_ptr<arrow::DataType> t_float,
             std::shared_ptr<arrow::io::FileOutputStream> &output_file,
             const arrow::ipc::IpcWriteOptions &options) {
  unsigned long thisentries = 0;
  uint64_t header, data[255];
  uint16_t run, bx;
  uint32_t orbit;
  uint8_t npuppi;
  //puppi candidate info:
  float pt[255];
  float eta[255], phi[255];
  short int pdgid[255];
  //charged only:
  float z0[255];
  float dxy[255];
  //neutral only:
  float wpuppi[255];
  //common only:
  uint8_t quality[255];

  auto f_run = arrow::field("run", arrow::uint16());
  auto f_orbit = arrow::field("orbit", arrow::uint32());
  auto f_bx = arrow::field("bx", arrow::uint16());

  auto f_pt = arrow::field("pt", t_float);
  auto f_eta = arrow::field("eta", t_float);
  auto f_phi = arrow::field("phi", t_float);
  auto f_z0 = arrow::field("z0", t_float);
  auto f_dxy = arrow::field("dxy", t_float);
  auto f_wpuppi = arrow::field("wpuppi", t_float);
  auto f_pdgid = arrow::field("pdgid", arrow::int16());
  auto f_quality = arrow::field("quality", arrow::uint8());

  auto t_puppi = arrow::struct_({f_pt, f_eta, f_phi, f_z0, f_dxy, f_wpuppi, f_pdgid, f_quality});
  auto f_puppi = arrow::field("Puppi", arrow::list(t_puppi));

  auto schema = arrow::schema({f_run, f_orbit, f_bx, f_puppi});

  auto b_run = std::make_shared<arrow::UInt16Builder>();
  auto b_orbit = std::make_shared<arrow::UInt32Builder>();
  auto b_bx = std::make_shared<arrow::UInt16Builder>();
  auto b_pt_i = std::make_shared<FltBuilderType>();
  auto b_eta_i = std::make_shared<FltBuilderType>();
  auto b_phi_i = std::make_shared<FltBuilderType>();
  auto b_z0_i = std::make_shared<FltBuilderType>();
  auto b_dxy_i = std::make_shared<FltBuilderType>();
  auto b_wpuppi_i = std::make_shared<FltBuilderType>();
  auto b_pdgid_i = std::make_shared<arrow::Int16Builder>();
  auto b_quality_i = std::make_shared<arrow::UInt8Builder>();
  std::vector<std::shared_ptr<arrow::ArrayBuilder>> b_fields = {
      b_pt_i, b_eta_i, b_phi_i, b_z0_i, b_dxy_i, b_wpuppi_i, b_pdgid_i, b_quality_i};
  auto b_puppi_i = std::make_shared<arrow::StructBuilder>(t_puppi, arrow::default_memory_pool(), b_fields);
  arrow::ListBuilder b_puppi(arrow::default_memory_pool(), b_puppi_i);

  std::shared_ptr<arrow::ipc::RecordBatchWriter> batch_writer;
  if (output_file)
    batch_writer = *arrow::ipc::MakeFileWriter(output_file, schema, options);

  while (fin.good()) {
    readevent(fin, header, run, bx, orbit, npuppi, data, pt, eta, phi, pdgid, z0, dxy, wpuppi, quality);
    b_run->Append(run);
    b_orbit->Append(orbit);
    b_bx->Append(bx);
    b_puppi.Append();
    b_puppi_i->AppendValues(npuppi, nullptr);
    b_pt_i->AppendValues(pt, pt + npuppi);
    b_eta_i->AppendValues(eta, eta + npuppi);
    b_phi_i->AppendValues(phi, phi + npuppi);
    b_z0_i->AppendValues(z0, z0 + npuppi);
    b_dxy_i->AppendValues(dxy, dxy + npuppi);
    b_wpuppi_i->AppendValues(wpuppi, wpuppi + npuppi);
    b_pdgid_i->AppendValues(pdgid, npuppi);
    b_quality_i->AppendValues(quality, npuppi);
    entries++;
    thisentries++;
    if (batch_writer && (thisentries == batchsize || !fin.good())) {
      auto a_run = b_run->Finish();
      auto a_orbit = b_orbit->Finish();
      auto a_bx = b_bx->Finish();
      auto a_puppi = b_puppi.Finish();
      std::shared_ptr<arrow::RecordBatch> batch =
          arrow::RecordBatch::Make(schema, thisentries, {*a_run, *a_orbit, *a_bx, *a_puppi});
      batch_writer->WriteRecordBatch(*batch);
      thisentries = 0;
      nbatches++;
    }
  }
  if (output_file)
    batch_writer->Close();
}

void usage() {
  printf("Usage: example.exe [options] <layout> infile.dump [ outfile.arrow [ compression level ] ]\n");
  printf("   layout := ipc_float | ipc_float16 \n");
  printf("   compression := lz4 | zstd\n");
  printf("Options:\n");
  printf("  -b, --batchsize N: number of BXs per batch, default is 3564, one orbit at TM1 \n");
  printf("  -j, --iothreads N: set the capacity of the global I/O thread pool\n");
}

int main(int argc, char **argv) {
  if (argc < 3) {
    usage();
    return 1;
  }
  unsigned long batchsize = 3564;  // default is number of BXs in one LHC orbit
  unsigned int iothreads = -1;
  while (1) {
    static struct option long_options[] = {{"help", no_argument, nullptr, 'h'},
                                           {"batchsize", required_argument, nullptr, 'b'},
                                           {"iothreads", required_argument, nullptr, 'j'},
                                           {nullptr, 0, nullptr, 0}};
    int option_index = 0;
    int optc = getopt_long(argc, argv, "hj:b:", long_options, &option_index);
    if (optc == -1)
      break;

    switch (optc) {
      case 'h':
        usage();
        return 0;
      case 'b':
        batchsize = std::atoi(optarg);
        break;
      case 'j':
        iothreads = std::atoi(optarg);
        arrow::io::SetIOThreadPoolCapacity(iothreads);
        break;
      default:
        usage();
        return 1;
    }
  }
  int iarg = optind, narg = argc - optind;

  const char *inname = argv[iarg + 1], *outname = nullptr;
  std::fstream fin(inname, std::ios_base::in | std::ios_base::binary);
  if (!fin.good()) {
    printf("Error opening %s\n", inname);
    return 2;
  }
  std::string layout = std::string(argv[iarg]);
  printf("Will run with layout %s\n", argv[iarg]);
  unsigned long entries = 0, nbatches = 0;
  std::shared_ptr<arrow::io::FileOutputStream> output_file;
  if (narg >= 3) {
    outname = argv[iarg + 2];
    printf("Writing to %s\n", outname);
    output_file = *arrow::io::FileOutputStream::Open(outname);
  }
  std::chrono::time_point<std::chrono::steady_clock> tstart, tend;
  tstart = std::chrono::steady_clock::now();
  arrow::ipc::IpcWriteOptions ipcWriteOptions = arrow::ipc::IpcWriteOptions::Defaults();
  if (layout.find("ipc_") == 0) {
    if (narg >= 5) {
      int lvl = std::atoi(argv[iarg + 4]);
      std::string algo(argv[iarg + 3]);
      if (algo == "lz4")
        ipcWriteOptions.codec = *arrow::util::Codec::Create(arrow::Compression::LZ4_FRAME, lvl);
      else if (algo == "zstd")
        ipcWriteOptions.codec = *arrow::util::Codec::Create(arrow::Compression::ZSTD, lvl);
      else {
        printf("Unknown compression %s\n", algo.c_str());
        return 2;
      }
      printf("Enabled compression %s at level %d\n", algo.c_str(), lvl);
    }
  }

  if (layout == "ipc_float") {
    run_ipc<arrow::FloatType>(fin, entries, nbatches, batchsize, arrow::float32(), output_file, ipcWriteOptions);
  } else if (layout == "ipc_float16") {
    run_ipc<arrow::HalfFloatType>(fin, entries, nbatches, batchsize, arrow::float16(), output_file, ipcWriteOptions);
  }
  tend = std::chrono::steady_clock::now();
  double time = (std::chrono::duration<double>(tend - tstart)).count();
  if (batchsize)
    printf("Wrote %lu record batches of size %lu, %.1f kHz\n", nbatches, batchsize, nbatches / 1000. / time);
  report(time, time, inname, outname, entries);
}