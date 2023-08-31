
#include <TFile.h>
#include <Compression.h>
#include <TROOT.h>
#include <ROOT/RNTuple.hxx>
#include <ROOT/RNTupleModel.hxx>
#include <ROOT/RNTupleUtil.hxx>
#include <ROOT/RLogger.hxx>
#include <TStopwatch.h>
#include <string>
#include "unpack.h"
#include "puppi.h"

void usage() {
  printf("Usage: rntuple_unpack.exe [-j N] <layout> <type> infile.dump [ outfile.root [ <compression> <level> ]\n");
  printf("  layout := combined | combined_coll | combined_struct\n");
  printf("  type   := float\n");
  printf("  compression := lzma | zlib | lz4 | zstd\n");
}
int main(int argc, char **argv) {
  if (argc < 3) {
    return 1;
  }
  auto verbosity =
      ROOT::Experimental::RLogScopedVerbosity(ROOT::Experimental::NTupleLog(), ROOT::Experimental::ELogLevel::kError);
  int iarg = 1, narg = argc - 1;
  if (std::string(argv[iarg]) == "-j") {
    ROOT::EnableImplicitMT(std::stoi(argv[iarg + 1]));
    printf("Enabled Implicit MT with %d threads\n", std::stoi(argv[iarg + 1]));
    iarg += 2;
    narg -= 2;
  }
  std::string method = std::string(argv[iarg]);
  std::string type = std::string(argv[iarg + 1]);
  printf("Will run RTNuple with method %s, type %s\n", argv[iarg], argv[iarg + 1]);
  std::fstream fin(argv[iarg + 2], std::ios_base::in | std::ios_base::binary);
  if (!fin.good()) {
    printf("Error opening %s\n", argv[iarg + 2]);
    return 2;
  }

  int compressionLevel = 0;
  int compression = 0;
  if (narg >= 6) {
    std::string compressionName(argv[iarg + 4]);
    compressionLevel = std::stoi(argv[iarg + 5]);
    if (compressionName == "lzma")
      compression = ROOT::CompressionSettings(ROOT::kLZMA, compressionLevel);
    else if (compressionName == "zlib")
      compression = ROOT::CompressionSettings(ROOT::kZLIB, compressionLevel);
    else if (compressionName == "lz4")
      compression = ROOT::CompressionSettings(ROOT::kLZ4, compressionLevel);
    else if (compressionName == "zstd")
      compression = ROOT::CompressionSettings(ROOT::kZSTD, compressionLevel);
    else {
      printf("Unsupported compression algo %s\n", argv[iarg + 4]);
      return 1;
    }
  }

  const char *foutname = (narg > 3) ? argv[iarg + 3] : nullptr;

  TStopwatch timer;
  unsigned long entries = 0;
  if (type == "float" && method == "combined") {
    uint64_t header, data[255];

    auto model = ROOT::Experimental::RNTupleModel::Create();
    auto p_run = model->MakeField<uint16_t>("run");
    auto p_orbit = model->MakeField<uint32_t>("orbit");
    auto p_bx = model->MakeField<uint16_t>("bx");
    auto p_good = model->MakeField<bool>("good");
    auto p_npuppi = model->MakeField<uint8_t>("nPuppi");
    auto p_pt = model->MakeField<std::vector<float>>("Puppi_pt");
    auto p_eta = model->MakeField<std::vector<float>>("Puppi_eta");
    auto p_phi = model->MakeField<std::vector<float>>("Puppi_phi");
    auto p_pdgid = model->MakeField<std::vector<short int>>("Puppi_pdgId");
    auto p_z0 = model->MakeField<std::vector<float>>("Puppi_z0");
    auto p_dxy = model->MakeField<std::vector<float>>("Puppi_dxy");
    auto p_wpuppi = model->MakeField<std::vector<float>>("Puppi_wpuppi");
    auto p_quality = model->MakeField<std::vector<uint8_t>>("Puppi_quality");

    std::unique_ptr<ROOT::Experimental::RNTupleWriter> writer;
    if (foutname) {
      ROOT::Experimental::RNTupleWriteOptions options;
      options.SetCompression(compression);
      writer = ROOT::Experimental::RNTupleWriter::Recreate(std::move(model), "Events", foutname, options);
    }
    timer.Start();
    while (fin.good()) {
      readheader(fin, header, *p_run, *p_bx, *p_orbit, *p_good, *p_npuppi);
      if (header == 0)
        continue;  // skip null padding
      unsigned int npuppi = *p_npuppi;
      if (npuppi)
        fin.read(reinterpret_cast<char *>(&data[0]), npuppi * sizeof(uint64_t));
      p_pt->resize(npuppi);
      p_eta->resize(npuppi);
      p_phi->resize(npuppi);
      p_pdgid->resize(npuppi);
      p_z0->resize(npuppi);
      p_dxy->resize(npuppi);
      p_wpuppi->resize(npuppi);
      p_quality->resize(npuppi);
      for (unsigned int i = 0, n = npuppi; i < n; ++i) {
        readshared(data[i], (*p_pt)[i], (*p_eta)[i], (*p_phi)[i]);
        if (readpid(data[i], (*p_pdgid)[i])) {
          readcharged(data[i], (*p_z0)[i], (*p_dxy)[i], (*p_quality)[i]);
          (*p_wpuppi)[i] = 0;
        } else {
          readneutral(data[i], (*p_wpuppi)[i], (*p_quality)[i]);
          (*p_z0)[i] = 0;
          (*p_dxy)[i] = 0;
        }
      }
      if (writer)
        writer->Fill();
      entries++;
    }
  } else if (type == "float" && method == "combined_coll") {
    uint64_t header, data[255];

    auto submodel = ROOT::Experimental::RNTupleModel::Create();
    auto p_pt = submodel->MakeField<float>("pt");
    auto p_eta = submodel->MakeField<float>("eta");
    auto p_phi = submodel->MakeField<float>("phi");
    auto p_pdgid = submodel->MakeField<short int>("pdgId");
    auto p_z0 = submodel->MakeField<float>("z0");
    auto p_dxy = submodel->MakeField<float>("dxy");
    auto p_wpuppi = submodel->MakeField<float>("wpuppi");
    auto p_quality = submodel->MakeField<uint8_t>("quality");

    auto model = ROOT::Experimental::RNTupleModel::Create();
    auto p_run = model->MakeField<uint16_t>("run");
    auto p_orbit = model->MakeField<uint32_t>("orbit");
    auto p_bx = model->MakeField<uint16_t>("bx");
    auto p_good = model->MakeField<bool>("good");
    auto c_puppi = model->MakeCollection("Puppi", std::move(submodel));

    std::unique_ptr<ROOT::Experimental::RNTupleWriter> writer;
    if (foutname) {
      ROOT::Experimental::RNTupleWriteOptions options;
      options.SetCompression(compression);
      writer = ROOT::Experimental::RNTupleWriter::Recreate(std::move(model), "Events", foutname, options);
    }
    uint16_t npuppi;
    timer.Start();
    while (fin.good()) {
      readheader(fin, header, *p_run, *p_bx, *p_orbit, *p_good, npuppi);
      if (header == 0)
        continue;  // skip null padding
      if (npuppi)
        fin.read(reinterpret_cast<char *>(&data[0]), npuppi * sizeof(uint64_t));
      for (unsigned int i = 0, n = npuppi; i < n; ++i) {
        readshared(data[i], *p_pt, *p_eta, *p_phi);
        if (readpid(data[i], *p_pdgid)) {
          readcharged(data[i], *p_z0, *p_dxy, *p_quality);
          *p_wpuppi = 0;
        } else {
          readneutral(data[i], *p_wpuppi, *p_quality);
          *p_z0 = 0;
          *p_dxy = 0;
        }
        if (writer)
          c_puppi->Fill();
      }
      if (writer)
        writer->Fill();
      entries++;
    }
  } else if (type == "float" && method == "combined_struct") {
    uint64_t header, data[255];

    auto model = ROOT::Experimental::RNTupleModel::Create();
    auto p_run = model->MakeField<uint16_t>("run");
    auto p_orbit = model->MakeField<uint32_t>("orbit");
    auto p_bx = model->MakeField<uint16_t>("bx");
    auto p_good = model->MakeField<bool>("good");
    auto p_puppi = model->MakeField<std::vector<Puppi>>("Puppi");

    std::unique_ptr<ROOT::Experimental::RNTupleWriter> writer;
    if (foutname) {
      ROOT::Experimental::RNTupleWriteOptions options;
      options.SetCompression(compression);
      writer = ROOT::Experimental::RNTupleWriter::Recreate(std::move(model), "Events", foutname, options);
    }
    uint16_t npuppi;
    timer.Start();
    while (fin.good()) {
      readheader(fin, header, *p_run, *p_bx, *p_orbit, *p_good, npuppi);
      if (header == 0)
        continue;  // skip null padding
      if (npuppi)
        fin.read(reinterpret_cast<char *>(&data[0]), npuppi * sizeof(uint64_t));
      p_puppi->resize(npuppi);
      for (unsigned int i = 0, n = npuppi; i < n; ++i) {
        Puppi pup;
        readshared(data[i], pup.pt, pup.eta, pup.phi);
        if (readpid(data[i], pup.pid)) {
          readcharged(data[i], pup.z0, pup.dxy, pup.quality);
          pup.wpuppi = 0;
        } else {
          readneutral(data[i], pup.wpuppi, pup.quality);
          pup.z0 = 0;
          pup.dxy = 0;
        }
        (*p_puppi)[i] = pup;
      }
      if (writer)
        writer->Fill();
      entries++;
    }
  } else if (type == "raw64") {
    auto model = ROOT::Experimental::RNTupleModel::Create();
    auto p_run = model->MakeField<uint16_t>("run");
    auto p_orbit = model->MakeField<uint32_t>("orbit");
    auto p_bx = model->MakeField<uint16_t>("bx");
    auto p_good = model->MakeField<bool>("good");
    auto p_puppi = model->MakeField<std::vector<uint64_t>>("Puppi");
    uint16_t npuppi;
    uint64_t header;

    std::unique_ptr<ROOT::Experimental::RNTupleWriter> writer;
    if (foutname) {
      ROOT::Experimental::RNTupleWriteOptions options;
      options.SetCompression(compression);
      writer = ROOT::Experimental::RNTupleWriter::Recreate(std::move(model), "Events", foutname, options);
    }
    timer.Start();
    while (fin.good()) {
      readheader(fin, header, *p_run, *p_bx, *p_orbit, *p_good, npuppi);
      if (header == 0)
        continue;  // skip null padding
      p_puppi->resize(npuppi);
      if (npuppi)
        fin.read(reinterpret_cast<char *>(&*p_puppi->begin()), npuppi * sizeof(uint64_t));

      if (writer)
        writer->Fill();
      entries++;
    }
  } else {
    usage();
    return 4;
  }
  timer.Stop();
  double tcpu = timer.CpuTime(), treal = timer.RealTime();
  report(tcpu, treal, entries, argv[iarg + 2], narg > 3 ? argv[iarg + 3] : nullptr);
  return 0;
}