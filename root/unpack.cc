#include <cstdio>
#include <cstdint>
#include <vector>
#include <TTree.h>
#include <TFile.h>
#include <Compression.h>
#include <TROOT.h>
#include <TStopwatch.h>
#include <string>
#include <getopt.h>

#include "unpack.h"

void usage() {
  printf("Usage: unpack.exe [ options ] <layout> <type> infile.dump [ outfile.root ]\n");
  printf("  layout := separate | combined\n");
  printf("  type   := float | float24 | int | raw64\n");
  printf("Options: \n");
  printf("  -j N            : multithread with N threads\n");
  printf("  -z algo[,level] : enable compression\n");
  printf("                    algorithms supported are none, lzma, zlib, lz4, zstd;\n");
  printf("                    default level is 4\n");
}
int main(int argc, char **argv) {
  if (argc < 3) {
    usage();
    return 1;
  }
  int compressionAlgo, compressionLevel = 0;
  while (1) {
    static struct option long_options[] = {{"help", no_argument, nullptr, 'h'},
                                           {"threads", required_argument, nullptr, 'j'},
                                           {"compression", required_argument, nullptr, 'z'},
                                           {nullptr, 0, nullptr, 0}};
    int option_index = 0;
    int optc = getopt_long(argc, argv, "hj:z:", long_options, &option_index);
    if (optc == -1)
      break;

    switch (optc) {
      case 'h':
        usage();
        return 0;
      case 'j': {
        int threads = std::atoi(optarg);
        if (threads != -1) {
          ROOT::EnableImplicitMT(threads);
          printf("Enabled Implicit MT with %d threads\n", threads);
        }
      } break;
      case 'z': {
        std::string compressionMethod = std::string(optarg);
        auto pos = compressionMethod.find(",");
        if (pos != std::string::npos) {
          compressionLevel = std::atoi(compressionMethod.substr(pos + 1).c_str());
          compressionMethod = compressionMethod.substr(0, pos);
        } else {
          compressionLevel = 4;
        }
        if (compressionMethod == "none") {
          compressionAlgo = ROOT::RCompressionSetting::EAlgorithm::kZLIB;
          compressionLevel = 0;
        } else if (compressionMethod == "lzma")
          compressionAlgo = ROOT::RCompressionSetting::EAlgorithm::kLZMA;
        else if (compressionMethod == "zlib")
          compressionAlgo = ROOT::RCompressionSetting::EAlgorithm::kZLIB;
        else if (compressionMethod == "lz4")
          compressionAlgo = ROOT::RCompressionSetting::EAlgorithm::kLZ4;
        else if (compressionMethod == "zstd")
          compressionAlgo = ROOT::RCompressionSetting::EAlgorithm::kZSTD;
        else {
          printf("Unsupported compression algo %s\n", optarg);
          return 1;
        }
      } break;
      default:
        usage();
        return 1;
    }
  }

  int iarg = optind, narg = argc - optind;
  std::string method = std::string(argv[iarg]), type = std::string(argv[iarg + 1]);
  printf("Will run with method %s, type %s\n", argv[iarg], argv[iarg + 1]);
  std::fstream fin(argv[iarg + 2], std::ios_base::in | std::ios_base::binary);
  if (!fin.good()) {
    printf("Error opening %s\n", argv[iarg + 2]);
    return 2;
  }
  TFile *fout = nullptr;
  TTree *tree = nullptr;
  std::string output;
  for (int i = iarg+3; i < iarg + narg; ++i) {
    std::string fname = argv[i];
    if (fname.length() > 5 && fname.substr(fname.length()-5) == ".root") {
      if (fout != nullptr) {
        printf("Multiple output root files specified in the command line\n");
        return 2;
      }
      output = fname;
      fout = TFile::Open(argv[i], "RECREATE", "", compressionLevel);
      if (fout == nullptr || !fout) {
        printf("Error opening root file %s for output\n", argv[i]);
        return 2;
      }
      if (compressionLevel)
        fout->SetCompressionAlgorithm(compressionAlgo);
      tree = new TTree("Events", "Events");
    }
  }

  TStopwatch timer;
  unsigned long entries = 0;
  if (type == "int" && method == "combined") {
    uint64_t header, data[255];
    uint16_t run, bx;
    uint32_t orbit;
    Bool_t good;
    uint16_t npuppi;  // issues with uint8_t that root sees to max at 127
    //puppi candidate info:
    uint16_t pt[255];
    int16_t eta[255], phi[255];
    uint16_t pdgid[255];  //this is really pid but we need to call it pdg
    //charged only:
    int16_t z0[255];
    int8_t dxy[255];
    uint16_t quality[255];
    //neutral only:
    uint16_t wpuppi[255];
    uint16_t id[255];

    if (tree) {
      tree->Branch("run", &run, "run/s");
      tree->Branch("orbit", &orbit, "orbit/i");
      tree->Branch("bx", &bx, "bx/s");
      tree->Branch("good", &good, "good/O");
      tree->Branch("nPuppi", &npuppi, "nPuppi/s");
      tree->Branch("Puppi_pt", &pt, "Puppi_pt[nPuppi]/s");
      tree->Branch("Puppi_eta", &eta, "Puppi_eta[nPuppi]/S");
      tree->Branch("Puppi_phi", &phi, "Puppi_phi[nPuppi]/S");
      tree->Branch("Puppi_pid", &pdgid, "Puppi_pid[nPuppi]/b");
      tree->Branch("Puppi_z0", &z0, "Puppi_z0[nPuppi]/S");
      tree->Branch("Puppi_dxy", &dxy, "Puppi_dxy[nPuppi]/B");
      tree->Branch("Puppi_quality", &quality, "Puppi_quality[nPuppi]/b");
      tree->Branch("Puppi_wpuppi", &wpuppi, "Puppi_wpuppi[nPuppi]/s");
      tree->Branch("Puppi_id", &id, "Puppi_id[nPuppi]/b");
    }

    timer.Start();
    while (fin.good()) {
      readevent(fin, header, run, bx, orbit, good, npuppi, data, pt, eta, phi, pdgid, z0, dxy, quality, wpuppi, id);
      if (header == 0)
        continue;  // skip null padding
      if (tree)
        tree->Fill();
      entries++;
    }
  } else if (type == "int" && method == "separate") {
    uint64_t header, data[255];
    uint16_t run, bx;
    uint32_t orbit;
    Bool_t good;
    uint16_t npuppi, npuppi_c, npuppi_n;  // issues with uint8_t that root sees to max at 127
    //puppi candidate info:
    uint16_t pt_c[255], pt_n[255];
    int16_t eta_c[255], eta_n[255], phi_c[255], phi_n[255];
    uint16_t pdgid_c[255], pdgid_n[255];  //this is really pid but we need to give it the same name
    //charged only:
    int16_t z0[255];
    int8_t dxy[255];
    uint16_t quality[255];
    //neutral only:
    uint16_t wpuppi[255];
    uint16_t id[255];

    if (tree) {
      tree->Branch("run", &run, "run/s");
      tree->Branch("orbit", &orbit, "orbit/i");
      tree->Branch("bx", &bx, "bx/s");
      tree->Branch("good", &good, "good/O");
      tree->Branch("nPuppi", &npuppi, "nPuppi/s");
      //charged branches
      tree->Branch("nPuppiCharged", &npuppi_c, "nPuppiCharged/s");
      tree->Branch("PuppiCharged_pt", &pt_c, "pt_c[nPuppiCharged]/s");
      tree->Branch("PuppiCharged_eta", &eta_c, "eta_c[nPuppiCharged]/S");
      tree->Branch("PuppiCharged_phi", &phi_c, "phi_c[nPuppiCharged]/S");
      tree->Branch("PuppiCharged_pid", &pdgid_c, "PuppiCharged_pid[nPuppiCharged]/b");
      tree->Branch("PuppiCharged_z0", &z0, "PuppiCharged_z0[nPuppiCharged]/S");
      tree->Branch("PuppiCharged_dxy", &dxy, "PuppiCharged_dxy[nPuppiCharged]/B");
      tree->Branch("PuppiCharged_quality", &quality, "PuppiCharged_quality[nPuppi]/b");
      //neutral branches
      tree->Branch("nPuppiNeutral", &npuppi_n, "nPuppiNeutral/s");
      tree->Branch("PuppiNeutral_pt", &pt_n, "PuppiNeutral_pt[nPuppiNeutral]/s");
      tree->Branch("PuppiNeutral_eta", &eta_n, "PuppiNeutral_eta[nPuppiNeutral]/S");
      tree->Branch("PuppiNeutral_phi", &phi_n, "PuppiNeutral_phi[nPuppiCharged]/S");
      tree->Branch("PuppiNeutral_pid", &pdgid_n, "PuppiNeutral_pid[nPuppiNeutral]/b");
      tree->Branch("PuppiNeutral_wpuppi", &wpuppi, "PuppiNeutral_wpuppi[nPuppiNeutral]/s");
      tree->Branch("PuppiNeutral_id", &id, "PuppiNeutral_id[nPuppi]/b");
    }

    timer.Start();
    while (fin.good()) {
      readevent(fin,
                header,
                run,
                bx,
                orbit,
                good,
                npuppi,
                npuppi_c,
                npuppi_n,
                data,
                pt_c,
                pt_n,
                eta_c,
                eta_n,
                phi_c,
                phi_n,
                pdgid_c,
                pdgid_n,
                z0,
                dxy,
                quality,
                wpuppi,
                id);
      if (header == 0)
        continue;  // skip null padding
      if (tree)
        tree->Fill();
      entries++;
    }

  } else if ((type == "float" || type == "float24") && method == "combined") {
    uint64_t header, data[255];
    uint16_t run, bx;
    uint32_t orbit;
    Bool_t good;
    uint8_t npuppi8;  // issues with uint8_t that root sees to max at 127
    uint16_t npuppi16;
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
    std::string F = (type == "float24" ? "f" : "F");

    if (tree) {
      tree->Branch("run", &run, "run/s");
      tree->Branch("orbit", &orbit, "orbit/i");
      tree->Branch("bx", &bx, "bx/s");
      tree->Branch("good", &good, "good/O");
      tree->Branch("nPuppi", &npuppi16, "nPuppi/s");
      tree->Branch("Puppi_pt", &pt, ("Puppi_pt[nPuppi]/" + F).c_str());
      tree->Branch("Puppi_eta", &eta, ("Puppi_eta[nPuppi]/" + F).c_str());
      tree->Branch("Puppi_phi", &phi, ("Puppi_phi[nPuppi]/" + F).c_str());
      tree->Branch("Puppi_pdgId", &pdgid, "Puppi_pdgId[nPuppi]/S");
      tree->Branch("Puppi_z0", &z0, ("Puppi_z0[nPuppi]/" + F).c_str());
      tree->Branch("Puppi_dxy", &dxy, ("Puppi_dxy[nPuppi]/" + F).c_str());
      tree->Branch("Puppi_wpuppi", &wpuppi, ("Puppi_wpuppi[nPuppi]/" + F).c_str());
      tree->Branch("Puppi_quality", &quality, "Puppi_quality[nPuppi]/b");
    }

    timer.Start();
    while (fin.good()) {
      readevent(fin, header, run, bx, orbit, good, npuppi8, data, pt, eta, phi, pdgid, z0, dxy, wpuppi, quality);
      if (header == 0)
        continue;  // skip null padding
      npuppi16 = npuppi8;
      if (tree)
        tree->Fill();
      entries++;
    }
  } else if ((type == "float" || type == "float24") && method == "separate") {
    uint64_t header, data[255];
    uint16_t run, bx;
    uint32_t orbit;
    Bool_t good;
    uint16_t npuppi, npuppi_c, npuppi_n;
    //puppi candidate info:
    float pt_c[255], pt_n[255], eta_c[255], eta_n[255], phi_c[255], phi_n[255];
    short int pdgid_c[255], pdgid_n[255];
    //charged only:
    float z0[255];
    float dxy[255];
    uint16_t quality[255];
    //neutral only:
    float wpuppi[255];
    uint16_t id[255];
    std::string F = (type == "float24" ? "f" : "F");

    if (tree) {
      tree->Branch("run", &run, "run/s");
      tree->Branch("orbit", &orbit, "orbit/i");
      tree->Branch("bx", &bx, "bx/s");
      tree->Branch("good", &good, "good/O");
      tree->Branch("nPuppi", &npuppi, "nPuppi/s");
      //charged branches
      tree->Branch("nPuppiCharged", &npuppi_c, "nPuppiCharged/s");
      tree->Branch("PuppiCharged_pt", &pt_c, ("PuppiCharged_pt[nPuppiCharged]/" + F).c_str());
      tree->Branch("PuppiCharged_eta", &eta_c, ("PuppiCharged_eta[nPuppiCharged]/" + F).c_str());
      tree->Branch("PuppiCharged_phi", &phi_c, ("PuppiCharged_phi[nPuppiCharged]/" + F).c_str());
      tree->Branch("PuppiCharged_pdgId", &pdgid_c, "PuppiCharged_pdgId[nPuppiCharged]/S");
      tree->Branch("PuppiCharged_quality", &quality, "PuppiCharged_quality[nPuppiCharged]/b");
      tree->Branch("PuppiCharged_z0", &z0, ("PuppiCharged_z0[nPuppiCharged]/" + F).c_str());
      tree->Branch("PuppiCharged_dxy", &dxy, ("PuppiCharged_dxy[nPuppiCharged]/" + F).c_str());
      //neutral branches
      tree->Branch("nPuppiNeutral", &npuppi_n, "nPuppiNeutral/s");
      tree->Branch("PuppiNeutral_pt", &pt_n, ("PuppiNeutral_pt[nPuppiNeutral]/" + F).c_str());
      tree->Branch("PuppiNeutral_eta", &eta_n, ("PuppiNeutral_eta[nPuppiNeutral]/" + F).c_str());
      tree->Branch("PuppiNeutral_phi", &phi_n, ("PuppiNeutral_phi[nPuppiNeutral]/" + F).c_str());
      tree->Branch("PuppiNeutral_pdgId", &pdgid_n, "PuppiNeutral_pdgId[nPuppiNeutral]/S");
      tree->Branch("PuppiNeutral_wpuppi", &wpuppi, ("PuppiNeutral_wpuppi[nPuppiNeutral]/" + F).c_str());
      tree->Branch("PuppiNeutral_id", &id, "PuppiNeutral_id[nPuppiNeutral]/b");
    }

    timer.Start();
    while (fin.good()) {
      readevent(fin,
                header,
                run,
                bx,
                orbit,
                good,
                npuppi,
                npuppi_c,
                npuppi_n,
                data,
                pt_c,
                pt_n,
                eta_c,
                eta_n,
                phi_c,
                phi_n,
                pdgid_c,
                pdgid_n,
                z0,
                dxy,
                quality,
                wpuppi,
                id);
      if (header == 0)
        continue;  // skip null padding
      if (tree)
        tree->Fill();
      entries++;
    }
  } else if (type == "raw64" && method == "combined") {
    uint64_t header, data[255];
    uint16_t run, bx;
    uint32_t orbit;
    Bool_t good;
    uint8_t npuppi8;  // issues with uint8_t that root sees to max at 127
    uint16_t npuppi16;
    //puppi candidate info:
    if (tree) {
      tree->Branch("run", &run, "run/s");
      tree->Branch("orbit", &orbit, "orbit/i");
      tree->Branch("bx", &bx, "bx/s");
      tree->Branch("good", &good, "good/O");
      tree->Branch("nPuppi", &npuppi16, "nPuppi/s");
      tree->Branch("Puppi_packed", &data, "Puppi_packed[nPuppi]/l");
    }

    timer.Start();
    while (fin.good()) {
      readheader(fin, header, run, bx, orbit, good, npuppi8);
      if (header == 0)
        continue;  // skip null padding
      if (npuppi8)
        fin.read(reinterpret_cast<char *>(data), npuppi8 * sizeof(uint64_t));
      npuppi16 = npuppi8;
      if (tree)
        tree->Fill();
      entries++;
    }
  } else {
    usage();
    return 4;
  }
  if (tree) {
    tree->Write();
    fout->Close();
  }
  timer.Stop();
  double tcpu = timer.CpuTime(), treal = timer.RealTime();
  report(tcpu, treal, entries, argv[iarg + 2], output.empty() ? nullptr : output.c_str());
  return 0;
}