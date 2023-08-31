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
  bool good;
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
  auto f_good = arrow::field("good", arrow::boolean());

  auto f_pt = arrow::field("pt", t_float);
  auto f_eta = arrow::field("eta", t_float);
  auto f_phi = arrow::field("phi", t_float);
  auto f_z0 = arrow::field("z0", t_float);
  auto f_dxy = arrow::field("dxy", t_float);
  auto f_wpuppi = arrow::field("wpuppi", t_float);
  auto f_pdgid = arrow::field("pdgId", arrow::int16());
  auto f_quality = arrow::field("quality", arrow::uint8());

  auto t_puppi = arrow::struct_({f_pt, f_eta, f_phi, f_z0, f_dxy, f_wpuppi, f_pdgid, f_quality});
  auto f_puppi = arrow::field("Puppi", arrow::list(t_puppi));

  auto schema = arrow::schema({f_run, f_orbit, f_bx, f_good, f_puppi});

  auto b_run = std::make_shared<arrow::UInt16Builder>();
  auto b_orbit = std::make_shared<arrow::UInt32Builder>();
  auto b_bx = std::make_shared<arrow::UInt16Builder>();
  auto b_good = std::make_shared<arrow::BooleanBuilder>();
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
    readevent(fin, header, run, bx, orbit, good, npuppi, data, pt, eta, phi, pdgid, z0, dxy, wpuppi, quality);
    if (header == 0)
      continue;  // skip null padding
    b_run->Append(run);
    b_orbit->Append(orbit);
    b_bx->Append(bx);
    b_good->Append(good);
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
      auto a_good = b_good->Finish();
      auto a_puppi = b_puppi.Finish();
      std::shared_ptr<arrow::RecordBatch> batch =
          arrow::RecordBatch::Make(schema, thisentries, {*a_run, *a_orbit, *a_bx, *a_good, *a_puppi});
      batch_writer->WriteRecordBatch(*batch);
      thisentries = 0;
      nbatches++;
    }
  }
  if (output_file)
    batch_writer->Close();
}

template <typename FltType, typename FltArrayType = typename arrow::TypeTraits<FltType>::ArrayType>
void run_ipc_bulk(std::fstream &fin,
                  unsigned long &entries,
                  unsigned long &nbatches,
                  unsigned long batchsize,
                  std::shared_ptr<arrow::DataType> t_float,
                  std::shared_ptr<arrow::io::FileOutputStream> &output_file,
                  const arrow::ipc::IpcWriteOptions &options) {
  assert(batchsize != 0);
  std::vector<uint64_t> header(batchsize), data;
  std::vector<uint16_t> run(batchsize), bx(batchsize);
  std::vector<uint32_t> orbit(batchsize);
  std::vector<uint8_t> npuppi(batchsize);

  //puppi candidate info:
  std::vector<float> pt, eta, phi, z0, dxy, wpuppi;
  std::vector<int16_t> pdgid;
  std::vector<uint8_t> quality;

  auto f_run = arrow::field("run", arrow::uint16());
  auto f_orbit = arrow::field("orbit", arrow::uint32());
  auto f_bx = arrow::field("bx", arrow::uint16());
  auto f_good = arrow::field("good", arrow::boolean());

  // std::vector<bool> is tricky, so we just use a plain builder for this
  auto b_good = std::make_shared<arrow::BooleanBuilder>();

  auto f_pt = arrow::field("pt", t_float);
  auto f_eta = arrow::field("eta", t_float);
  auto f_phi = arrow::field("phi", t_float);
  auto f_z0 = arrow::field("z0", t_float);
  auto f_dxy = arrow::field("dxy", t_float);
  auto f_wpuppi = arrow::field("wpuppi", t_float);
  auto f_pdgid = arrow::field("pdgId", arrow::int16());
  auto f_quality = arrow::field("quality", arrow::uint8());

  auto t_puppi = arrow::struct_({f_pt, f_eta, f_phi, f_z0, f_dxy, f_wpuppi, f_pdgid, f_quality});
  auto t_puppis = arrow::list(t_puppi);
  auto f_puppi = arrow::field("Puppi", t_puppis);

  auto schema = arrow::schema({f_run, f_orbit, f_bx, f_good, f_puppi});

  std::shared_ptr<arrow::ipc::RecordBatchWriter> batch_writer;
  if (output_file)
    batch_writer = *arrow::ipc::MakeFileWriter(output_file, schema, options);

  unsigned int e = 0;
  std::vector<int> offsets(1, 0);
  unsigned int npuppitot = 0, capacity = batchsize;
  data.resize(capacity);
  bool good;
  while (fin.good()) {
    readheader(fin, header[e], run[e], bx[e], orbit[e], good, npuppi[e]);
    if (header[e] == 0)
      continue;  // skip null padding
    b_good->Append(good);
    unsigned int lastvalid = offsets.back();
    offsets.emplace_back(offsets.back() + npuppi[e]);
    if (unsigned(offsets.back()) >= capacity) {
      ;
      capacity = std::max<unsigned int>(offsets.back(), 2 * capacity);
      data.resize(capacity);
    }
    if (npuppi[e])
      fin.read(reinterpret_cast<char *>(&data[lastvalid]), npuppi[e] * sizeof(uint64_t));
    e++;
    entries++;
    if (e == batchsize || !fin.good()) {
      unsigned int nallpuppi = offsets.back();
      for (std::vector<float> *v : {&pt, &eta, &phi, &dxy, &z0, &wpuppi})
        v->resize(nallpuppi);
      pdgid.resize(nallpuppi);
      quality.resize(nallpuppi);
      for (unsigned int i = 0; i < nallpuppi; ++i) {
        readshared(data[i], pt[i], eta[i], phi[i]);
        if (readpid(data[i], pdgid[i])) {
          readcharged(data[i], z0[i], dxy[i], quality[i]);
          wpuppi[i] = 0;
        } else {
          readneutral(data[i], wpuppi[i], quality[i]);
          z0[i] = 0;
          dxy[i] = 0;
        }
      }
      std::shared_ptr<arrow::Array> a_run(new arrow::UInt16Array(e, arrow::Buffer::Wrap(run)));
      std::shared_ptr<arrow::Array> a_orbit(new arrow::UInt32Array(e, arrow::Buffer::Wrap(orbit)));
      std::shared_ptr<arrow::Array> a_bx(new arrow::UInt16Array(e, arrow::Buffer::Wrap(bx)));
      auto a_good = b_good->Finish();
      std::shared_ptr<arrow::Array> a_pt(new FltArrayType(nallpuppi, arrow::Buffer::Wrap(pt)));
      std::shared_ptr<arrow::Array> a_eta(new FltArrayType(nallpuppi, arrow::Buffer::Wrap(eta)));
      std::shared_ptr<arrow::Array> a_phi(new FltArrayType(nallpuppi, arrow::Buffer::Wrap(phi)));
      std::shared_ptr<arrow::Array> a_dxy(new FltArrayType(nallpuppi, arrow::Buffer::Wrap(dxy)));
      std::shared_ptr<arrow::Array> a_z0(new FltArrayType(nallpuppi, arrow::Buffer::Wrap(z0)));
      std::shared_ptr<arrow::Array> a_wpuppi(new FltArrayType(nallpuppi, arrow::Buffer::Wrap(wpuppi)));
      std::shared_ptr<arrow::Array> a_pdgid(new arrow::Int16Array(nallpuppi, arrow::Buffer::Wrap(pdgid)));
      std::shared_ptr<arrow::Array> a_quality(new arrow::UInt8Array(nallpuppi, arrow::Buffer::Wrap(quality)));
      std::shared_ptr<arrow::Array> a_flat_puppi(
          new arrow::StructArray(t_puppi, nallpuppi, {a_pt, a_eta, a_phi, a_z0, a_dxy, a_wpuppi, a_pdgid, a_quality}));
      std::shared_ptr<arrow::Array> a_puppi(
          new arrow::ListArray(t_puppis, e, arrow::Buffer::Wrap(offsets), a_flat_puppi));
      if (output_file) {
        std::shared_ptr<arrow::RecordBatch> batch =
            arrow::RecordBatch::Make(schema, e, {a_run, a_orbit, a_bx, *a_good, a_puppi});
        batch_writer->WriteRecordBatch(*batch);
      }
      e = 0;
      offsets.resize(1);
      nbatches++;
      b_good->Reset();
    }
  }
  if (output_file)
    batch_writer->Close();
}

void run_ipc_int_bulk(std::fstream &fin,
                      unsigned long &entries,
                      unsigned long &nbatches,
                      unsigned long batchsize,
                      std::shared_ptr<arrow::io::FileOutputStream> &output_file,
                      const arrow::ipc::IpcWriteOptions &options) {
  assert(batchsize != 0);
  std::vector<uint64_t> header(batchsize), data;
  std::vector<uint16_t> run(batchsize), bx(batchsize);
  std::vector<uint32_t> orbit(batchsize);
  std::vector<uint8_t> npuppi(batchsize);

  //puppi candidate info:
  std::vector<uint16_t> pt, wpuppi;
  std::vector<int8_t> dxy;
  std::vector<int16_t> pdgid, eta, phi, z0;
  std::vector<uint8_t> quality;

  auto f_run = arrow::field("run", arrow::uint16());
  auto f_orbit = arrow::field("orbit", arrow::uint32());
  auto f_bx = arrow::field("bx", arrow::uint16());
  auto f_good = arrow::field("good", arrow::boolean());

  // std::vector<bool> is tricky, so we just use a plain builder for this
  auto b_good = std::make_shared<arrow::BooleanBuilder>();

  auto f_pt = arrow::field("pt", arrow::uint16());
  auto f_eta = arrow::field("eta", arrow::int16());
  auto f_phi = arrow::field("phi", arrow::int16());
  auto f_z0 = arrow::field("z0", arrow::int16());
  auto f_dxy = arrow::field("dxy", arrow::int8());
  auto f_wpuppi = arrow::field("wpuppi", arrow::uint16());
  auto f_pdgid = arrow::field("pdgId", arrow::int16());
  auto f_quality = arrow::field("quality", arrow::uint8());

  auto t_puppi = arrow::struct_({f_pt, f_eta, f_phi, f_z0, f_dxy, f_wpuppi, f_pdgid, f_quality});
  auto t_puppis = arrow::list(t_puppi);
  auto f_puppi = arrow::field("Puppi", t_puppis);

  auto schema = arrow::schema({f_run, f_orbit, f_bx, f_good, f_puppi});

  std::shared_ptr<arrow::ipc::RecordBatchWriter> batch_writer;
  if (output_file)
    batch_writer = *arrow::ipc::MakeFileWriter(output_file, schema, options);

  unsigned int e = 0;
  std::vector<int> offsets(1, 0);
  unsigned int npuppitot = 0, capacity = batchsize;
  data.resize(capacity);
  bool good;
  while (fin.good()) {
    readheader(fin, header[e], run[e], bx[e], orbit[e], good, npuppi[e]);
    if (header[e] == 0)
      continue;  // skip null padding
    b_good->Append(good);
    unsigned int lastvalid = offsets.back();
    offsets.emplace_back(offsets.back() + npuppi[e]);
    if (unsigned(offsets.back()) >= capacity) {
      ;
      capacity = std::max<unsigned int>(offsets.back(), 2 * capacity);
      data.resize(capacity);
    }
    if (npuppi[e])
      fin.read(reinterpret_cast<char *>(&data[lastvalid]), npuppi[e] * sizeof(uint64_t));
    e++;
    entries++;
    if (e == batchsize || !fin.good()) {
      unsigned int nallpuppi = offsets.back();
      for (std::vector<uint16_t> *v : {&pt, &wpuppi})
        v->resize(nallpuppi);
      for (std::vector<int16_t> *v : {&eta, &phi, &z0, &pdgid})
        v->resize(nallpuppi);
      dxy.resize(nallpuppi);
      quality.resize(nallpuppi);
      for (unsigned int i = 0; i < nallpuppi; ++i) {
        readshared(data[i], pt[i], eta[i], phi[i]);
        if (readpid(data[i], pdgid[i])) {
          readcharged(data[i], z0[i], dxy[i], quality[i]);
          wpuppi[i] = 0;
        } else {
          readneutral(data[i], wpuppi[i], quality[i]);
          z0[i] = 0;
          dxy[i] = 0;
        }
      }
      std::shared_ptr<arrow::Array> a_run(new arrow::UInt16Array(e, arrow::Buffer::Wrap(run)));
      std::shared_ptr<arrow::Array> a_orbit(new arrow::UInt32Array(e, arrow::Buffer::Wrap(orbit)));
      std::shared_ptr<arrow::Array> a_bx(new arrow::UInt16Array(e, arrow::Buffer::Wrap(bx)));
      auto a_good = b_good->Finish();
      std::shared_ptr<arrow::Array> a_pt(new arrow::UInt16Array(nallpuppi, arrow::Buffer::Wrap(pt)));
      std::shared_ptr<arrow::Array> a_eta(new arrow::Int16Array(nallpuppi, arrow::Buffer::Wrap(eta)));
      std::shared_ptr<arrow::Array> a_phi(new arrow::Int16Array(nallpuppi, arrow::Buffer::Wrap(phi)));
      std::shared_ptr<arrow::Array> a_dxy(new arrow::Int16Array(nallpuppi, arrow::Buffer::Wrap(dxy)));
      std::shared_ptr<arrow::Array> a_z0(new arrow::Int8Array(nallpuppi, arrow::Buffer::Wrap(z0)));
      std::shared_ptr<arrow::Array> a_wpuppi(new arrow::UInt16Array(nallpuppi, arrow::Buffer::Wrap(wpuppi)));
      std::shared_ptr<arrow::Array> a_pdgid(new arrow::Int16Array(nallpuppi, arrow::Buffer::Wrap(pdgid)));
      std::shared_ptr<arrow::Array> a_quality(new arrow::UInt8Array(nallpuppi, arrow::Buffer::Wrap(quality)));
      std::shared_ptr<arrow::Array> a_flat_puppi(
          new arrow::StructArray(t_puppi, nallpuppi, {a_pt, a_eta, a_phi, a_z0, a_dxy, a_wpuppi, a_pdgid, a_quality}));
      std::shared_ptr<arrow::Array> a_puppi(
          new arrow::ListArray(t_puppis, e, arrow::Buffer::Wrap(offsets), a_flat_puppi));
      if (output_file) {
        std::shared_ptr<arrow::RecordBatch> batch =
            arrow::RecordBatch::Make(schema, e, {a_run, a_orbit, a_bx, *a_good, a_puppi});
        batch_writer->WriteRecordBatch(*batch);
      }
      e = 0;
      offsets.resize(1);
      nbatches++;
      b_good->Reset();
    }
  }
  if (output_file)
    batch_writer->Close();
}

void run_ipc_raw64_bulk(std::fstream &fin,
                        unsigned long &entries,
                        unsigned long &nbatches,
                        unsigned long batchsize,
                        std::shared_ptr<arrow::io::FileOutputStream> &output_file,
                        const arrow::ipc::IpcWriteOptions &options) {
  assert(batchsize != 0);
  std::vector<uint64_t> header(batchsize), data;
  std::vector<uint16_t> run(batchsize), bx(batchsize);
  std::vector<uint32_t> orbit(batchsize);
  std::vector<uint8_t> npuppi(batchsize);

  auto f_run = arrow::field("run", arrow::uint16());
  auto f_orbit = arrow::field("orbit", arrow::uint32());
  auto f_bx = arrow::field("bx", arrow::uint16());
  auto f_good = arrow::field("good", arrow::boolean());

  // std::vector<bool> is tricky, so we just use a plain builder for this
  auto b_good = std::make_shared<arrow::BooleanBuilder>();

  auto f_data = arrow::field("packed", arrow::uint64());

  auto t_puppis = arrow::list(f_data);
  auto f_puppi = arrow::field("Puppi", t_puppis);

  auto schema = arrow::schema({f_run, f_orbit, f_bx, f_good, f_puppi});

  std::shared_ptr<arrow::ipc::RecordBatchWriter> batch_writer;
  if (output_file)
    batch_writer = *arrow::ipc::MakeFileWriter(output_file, schema, options);

  unsigned int e = 0;
  std::vector<int> offsets(1, 0);
  unsigned int npuppitot = 0, capacity = batchsize;
  data.resize(capacity);
  bool good;
  while (fin.good()) {
    readheader(fin, header[e], run[e], bx[e], orbit[e], good, npuppi[e]);
    if (header[e] == 0)
      continue;  // skip null padding
    b_good->Append(good);
    unsigned int lastvalid = offsets.back();
    offsets.emplace_back(offsets.back() + npuppi[e]);
    if (unsigned(offsets.back()) >= capacity) {
      ;
      capacity = std::max<unsigned int>(offsets.back(), 2 * capacity);
      data.resize(capacity);
    }
    if (npuppi[e])
      fin.read(reinterpret_cast<char *>(&data[lastvalid]), npuppi[e] * sizeof(uint64_t));
    e++;
    entries++;
    if (e == batchsize || !fin.good()) {
      unsigned int nallpuppi = offsets.back();
      std::shared_ptr<arrow::Array> a_run(new arrow::UInt16Array(e, arrow::Buffer::Wrap(run)));
      std::shared_ptr<arrow::Array> a_orbit(new arrow::UInt32Array(e, arrow::Buffer::Wrap(orbit)));
      std::shared_ptr<arrow::Array> a_bx(new arrow::UInt16Array(e, arrow::Buffer::Wrap(bx)));
      auto a_good = b_good->Finish();
      std::shared_ptr<arrow::Array> a_packed(new arrow::UInt64Array(nallpuppi, arrow::Buffer::Wrap(data)));
      std::shared_ptr<arrow::Array> a_puppi(new arrow::ListArray(t_puppis, e, arrow::Buffer::Wrap(offsets), a_packed));
      if (output_file) {
        std::shared_ptr<arrow::RecordBatch> batch =
            arrow::RecordBatch::Make(schema, e, {a_run, a_orbit, a_bx, *a_good, a_puppi});
        batch_writer->WriteRecordBatch(*batch);
      }
      e = 0;
      offsets.resize(1);
      nbatches++;
      b_good->Reset();
    }
  }
  if (output_file)
    batch_writer->Close();
}

void usage() {
  printf("Usage: example.exe [options] <layout> infile.dump [ outfile.arrow [ compression level ] ]\n");
  printf("   layout := ipc_float | ipc_float16 | ipc_float_bulk | ipc_int_bulk | ipc_raw64_bulk \n");
  printf("   compression := lz4 | zstd\n");
  printf("Options:\n");
  printf("  -b, --batchsize N: number of BXs per batch, default is 3564, one orbit at TM1 \n");
  printf("  -j, --cputhreads N: set the capacity of the global CPU thread pool\n");
  printf("  -J, --iothreads N: set the capacity of the global I/O thread pool\n");
}

int main(int argc, char **argv) {
  if (argc < 3) {
    usage();
    return 1;
  }
  unsigned long batchsize = 3564;  // default is number of BXs in one LHC orbit
  unsigned int iothreads = -1, cputhreads = -1;
  while (1) {
    static struct option long_options[] = {{"help", no_argument, nullptr, 'h'},
                                           {"batchsize", required_argument, nullptr, 'b'},
                                           {"iothreads", required_argument, nullptr, 'J'},
                                           {"cputhreads", required_argument, nullptr, 'j'},
                                           {nullptr, 0, nullptr, 0}};
    int option_index = 0;
    int optc = getopt_long(argc, argv, "hj:J:b:", long_options, &option_index);
    if (optc == -1)
      break;

    switch (optc) {
      case 'h':
        usage();
        return 0;
      case 'b':
        batchsize = std::atoi(optarg);
        break;
      case 'J':
        iothreads = std::atoi(optarg);
        arrow::io::SetIOThreadPoolCapacity(iothreads);
        break;
      case 'j':
        cputhreads = std::atoi(optarg);
        arrow::SetCpuThreadPoolCapacity(cputhreads);
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
  unsigned long entries = 0, nbatches = 0;
  std::shared_ptr<arrow::io::FileOutputStream> output_file;
  if (narg >= 3) {
    outname = argv[iarg + 2];
    printf("Will run with layout %s, writing to %s\n", argv[iarg], outname);
    output_file = *arrow::io::FileOutputStream::Open(outname);
  } else {
    printf("Will run with layout %s\n", argv[iarg]);
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
  } else if (layout == "ipc_float_bulk") {
    run_ipc_bulk<arrow::FloatType>(fin, entries, nbatches, batchsize, arrow::float32(), output_file, ipcWriteOptions);
  } else if (layout == "ipc_float16_bulk") {
    run_ipc_bulk<arrow::HalfFloatType>(
        fin, entries, nbatches, batchsize, arrow::float16(), output_file, ipcWriteOptions);
  } else if (layout == "ipc_int_bulk") {
    run_ipc_int_bulk(fin, entries, nbatches, batchsize, output_file, ipcWriteOptions);
  } else if (layout == "ipc_raw64_bulk") {
    run_ipc_raw64_bulk(fin, entries, nbatches, batchsize, output_file, ipcWriteOptions);
  } else if (layout == "ipc_float16") {
    run_ipc<arrow::HalfFloatType>(fin, entries, nbatches, batchsize, arrow::float16(), output_file, ipcWriteOptions);
  }
  tend = std::chrono::steady_clock::now();
  double time = (std::chrono::duration<double>(tend - tstart)).count();
  if (batchsize)
    printf("Wrote %lu record batches of size %lu, %.1f kHz\n", nbatches, batchsize, nbatches / 1000. / time);
  report(time, time, entries, inname, outname);
}