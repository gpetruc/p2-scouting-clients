#include <cstdio>
#include <cstdint>
#include <vector>
#include <TTree.h>
#include <TFile.h>
#include <Compression.h>
#include <TROOT.h>
#include <TStopwatch.h>
#include <string>
#include "unpack.h"

void usage() {
  printf("Usage: unpack.exe [-j N] <layout> <type> infile.dump [ outfile.root [ <compression> <level> ]\n");
  printf("  layout := separate | combined\n");
  printf("  type   := float | float24 | int\n");
  printf("  compression := lzma | zlib | lz4 | zstd\n");
}
int main(int argc, char **argv) {
  if (argc < 3) {
    usage();
    return 1;
  }
  int iarg = 1, narg = argc - 1;
  if (std::string(argv[iarg]) == "-j") {
    ROOT::EnableImplicitMT(std::stoi(argv[iarg + 1]));
    printf("Enabled Implicit MT with %d threads\n", std::stoi(argv[iarg + 1]));
    iarg += 2;
    narg -= 2;
  }
  std::string method = std::string(argv[iarg]);
  std::string type = std::string(argv[iarg + 1]);
  printf("Will run with method %s, type %s\n", argv[iarg], argv[iarg + 1]);
  std::fstream fin(argv[iarg + 2], std::ios_base::in | std::ios_base::binary);
  if (!fin.good()) {
    printf("Error opening %s\n", argv[iarg + 2]);
    return 2;
  }

  int compressionAlgo, compressionLevel = 0;
  if (narg >= 6) {
    std::string compressionName(argv[iarg + 4]);
    if (compressionName == "lzma")
      compressionAlgo = ROOT::RCompressionSetting::EAlgorithm::kLZMA;
    else if (compressionName == "zlib")
      compressionAlgo = ROOT::RCompressionSetting::EAlgorithm::kZLIB;
    else if (compressionName == "lz4")
      compressionAlgo = ROOT::RCompressionSetting::EAlgorithm::kLZ4;
    else if (compressionName == "zstd")
      compressionAlgo = ROOT::RCompressionSetting::EAlgorithm::kZSTD;
    else {
      printf("Unsupported compression algo %s\n", argv[iarg + 2]);
      return 1;
    }
    compressionLevel = std::stoi(argv[iarg + 5]);
  }

  TFile *fout = nullptr;
  TTree *tree = nullptr;
  if (narg > 3) {
    fout = TFile::Open(argv[iarg + 3], "RECREATE", "", compressionLevel);
    if (fout == nullptr || !fout)
      return 2;
    if (compressionLevel)
      fout->SetCompressionAlgorithm(compressionAlgo);
    tree = new TTree("Events", "Events");
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
      tree->Branch("Puppi_pid", &pdgid, "Puppi_pid[nPuppi]/b");
      tree->Branch("Puppi_z0", &z0, ("Puppi_z0[nPuppi]/" + F).c_str());
      tree->Branch("Puppi_dxy", &dxy, ("Puppi_dxy[nPuppi]/" + F).c_str());
      tree->Branch("Puppi_wpuppi", &wpuppi, ("Puppi_wpuppi[nPuppi]/" + F).c_str());
      tree->Branch("Puppi_quality", &quality, "Puppi_quality[nPuppi]/b");
    }

    timer.Start();
    while (fin.good()) {
      readevent(fin, header, run, bx, orbit, good, npuppi8, data, pt, eta, phi, pdgid, z0, dxy, wpuppi, quality);
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
      tree->Branch("PuppiCharged_pid", &pdgid_c, "PuppiCharged_pid[nPuppiCharged]/b");
      tree->Branch("PuppiCharged_quality", &quality, "PuppiCharged_quality[nPuppiCharged]/b");
      tree->Branch("PuppiCharged_z0", &z0, ("PuppiCharged_z0[nPuppiCharged]/" + F).c_str());
      tree->Branch("PuppiCharged_dxy", &dxy, ("PuppiCharged_dxy[nPuppiCharged]/" + F).c_str());
      //neutral branches
      tree->Branch("nPuppiNeutral", &npuppi_n, "nPuppiNeutral/s");
      tree->Branch("PuppiNeutral_pt", &pt_n, ("PuppiNeutral_pt[nPuppiNeutral]/" + F).c_str());
      tree->Branch("PuppiNeutral_eta", &eta_n, ("PuppiNeutral_eta[nPuppiNeutral]/" + F).c_str());
      tree->Branch("PuppiNeutral_phi", &phi_n, ("PuppiNeutral_phi[nPuppiNeutral]/" + F).c_str());
      tree->Branch("PuppiNeutral_pid", &pdgid_n, "PuppiNeutral_pid[nPuppiNeutral]/b");
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
  report(tcpu, treal, argv[iarg + 2], narg > 3 ? argv[iarg + 3] : nullptr, entries);
  return 0;
}