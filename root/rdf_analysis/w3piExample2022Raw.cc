/**
 * Example W -> 3 pi+/- analysis code
 * 
 * Originally developed by Catherine Miller, Boston University, 2022
 * https://github.com/catherine-miller/w3pianalysis/tree/master
 * 
 * Modifications by Giovanni Petrucciani to run on different
 * input files formats, and slight optimization of the code.
 */

#include "w3piExample2022Raw.h"
#include "../../unpack.h"

#include <chrono>

w3piExample2022Raw::w3piExample2022Raw(const std::string &cutChoice, bool verbose)
    : w3piExample2022(cutChoice, verbose) {}

bool w3piExample2022Raw::analyzeRawEvent(
    uint16_t nwords, const ULong64_t *payload, Data &data, unsigned long int &npre, TreeFiller &filler) const {
  unsigned int size = nwords;
  w3piExample2022::vunpackPtFromRaw(payload, size, data.pt);
  const float *pts = data.pt;
  uint8_t ids[255];
  w3piExample2022::vunpackIDFromRaw(payload, size, ids);
  //this eliminates most of single neutrino events
  int lowcut = 0;
  int intermediatecut = 0;
  int highcut = 0;
  for (unsigned int i = 0; i < size; ++i) {
    if (ids[i] > 1 && ids[i] < 6) {
      if (pts[i] >= cuts.minpt1) {
        lowcut++;
        if (pts[i] >= cuts.minpt2) {
          intermediatecut++;
          if (pts[i] >= cuts.minpt3)
            highcut++;
        }
      }
    }
  }
  if (!(lowcut > 2 and intermediatecut > 1 and highcut > 0))
    return false;
  npre++;
  //SELECTION
  w3piExample2022::vunpackEtaFromRaw(payload, size, data.eta);
  w3piExample2022::vunpackPhiFromRaw(payload, size, data.phi);
  const float *etas = data.eta, *phis = data.phi;
  unsigned int npions = 0;
  unsigned int ix[255];  //pion indeces
  int icharge[255];      //pion charges
  int iso[255];          //pion isolation: 0 = not computed, 1 = pass, 2 = fail
  float bestPtSum = -1;

  for (unsigned int i = 0; i < size; ++i) {  //make list of all hadrons
    if (ids[i] > 1 && ids[i] < 6 and pts[i] >= cuts.minpt1) {
      ix[npions] = i;
      icharge[npions] = (ids[i] & 1) ? +1 : -1;
      npions++;
    }
  }
  std::fill(iso, iso + size, 0);

  if (npions > 2) {  //if there are 3+ pions
    float ptsum;

    for (unsigned int i1 = 0; i1 < npions; ++i1) {
      if (pts[ix[i1]] < cuts.minpt3)
        continue;  //high pt cut
      if (visolation(ix[i1], size, etas, phis, pts, cuts.mindr2, cuts.maxdr2, cuts.maxiso) == 0)
        continue;  //check iso of high pt pion
      iso[ix[i1]] = 1;
      for (unsigned int i2 = 0; i2 < npions; ++i2) {
        if (i2 == i1 || pts[ix[i2]] < cuts.minpt2)
          continue;
        if (pts[ix[i2]] > pts[ix[i1]] || (pts[ix[i2]] == pts[ix[i1]] and i2 < i1))
          continue;  //intermediate pt cut
        if (deltar(etas[ix[i1]], etas[ix[i2]], phis[ix[i1]], phis[ix[i2]], cuts.mindeltar2) == false)
          continue;  //angular sep of top 2 pions
        for (unsigned int i3 = 0; i3 < npions; ++i3) {
          if (i3 == i1 or i3 == i2)
            continue;
          if (pts[ix[i2]] < cuts.minpt1)
            continue;  //low pt cut
          if (pts[ix[i3]] > pts[ix[i1]] || (pts[ix[i3]] == pts[ix[i1]] and i3 < i1))
            continue;
          if (pts[ix[i3]] > pts[ix[i2]] || (pts[ix[i3]] == pts[ix[i2]] and i3 < i2))
            continue;
          unsigned int tr[3] = {ix[i1], ix[i2], ix[i3]};  //triplet of indeces
          if (std::abs(icharge[i1] + icharge[i2] + icharge[i3]) == 1) {
            //make Lorentz vectors for each triplet
            auto mass = vtripletmass(tr, pts, etas, phis);
            if (mass >= cuts.minmass and mass <= cuts.maxmass) {  //MASS test
              if (deltar(etas[ix[i1]], etas[ix[i3]], phis[ix[i1]], phis[ix[i3]], cuts.mindeltar2) == true and
                  deltar(etas[ix[i2]], etas[ix[i3]], phis[ix[i2]], phis[ix[i3]], cuts.mindeltar2) ==
                      true) {  //ANGULAR SEPARATION test
                //ISOLATION test for lower 2 pions
                bool isop = true;
                for (int j = 1; j < 3; ++j) {
                  if (iso[tr[j]] == 0) {
                    if (visolation(tr[j], size, etas, phis, pts, cuts.mindr2, cuts.maxdr2, cuts.maxiso) == false) {
                      iso[tr[j]] = 2;
                      isop = false;
                      break;
                    } else {
                      iso[tr[j]] = 1;
                    }
                  }
                  if (iso[tr[j]] == 2) {
                    isop = false;
                    break;  //fail triplet if one bad isolation
                  }
                }
                if (isop == true) {
                  ptsum = pts[tr[0]] + pts[tr[1]] + pts[tr[2]];
                  if (ptsum > bestPtSum) {
                    bestPtSum = ptsum;
                    filler.mass = mass;
                    std::copy(&tr[0], &tr[3], filler.indices);
                  }
                }  // iso
              }    // delta R
            }      // mass
          }        //charge
        }          //low pt cut
      }            //intermediate pt cut
    }              //high pt cut
  }                //if 3 or more pions

  if (bestPtSum < 0)
    return false;
  if (filler.tree) {
    w3piExample2022::vunpackZ0FromRaw(payload, ids, size, data.z0);
    w3piExample2022::vunpackDxyFromRaw(payload, ids, size, data.dxy);
    w3piExample2022::vunpackWPuppiFromRaw(payload, ids, size, data.wpuppi);
    w3piExample2022::vunpackQualityFromRaw(payload, ids, size, data.quality);
    w3piExample2022::vunpackPID(ids, size, data.pdgid);
    filler.tree->Fill();
  }
  return true;
}

rdfAnalysis::Report w3piExample2022Raw::run(const std::string & /*format*/,
                                            const std::vector<std::string> &infiles,
                                            const std::string &outformat,
                                            const std::string &outfile) const {
  auto tstart = std::chrono::steady_clock::now();

  auto masshist = new TH1D("masshist", "W Boson mass from selected pions; mass (GeV/c^2)", 100, 0., 100.);
  masshist->SetDirectory(nullptr);
  unsigned long int ntot = 0, npre = 0, npass = 0;
  TreeFiller filler;
  Data data;

  std::vector<std::fstream> fins;
  for (auto &in : infiles) {
    fins.emplace_back(in, std::ios_base::in | std::ios_base::binary);
    if (!fins.back().good()) {
      throw std::runtime_error("Error opening " + in + " for input");
    }
  }

  uint16_t run;
  uint32_t orbit;
  uint16_t bx;
  bool good;
  uint16_t nwords;
  uint64_t header;
  ULong64_t payload[1 << 12];

  if (outformat == "snapshot") {
    filler.tfile = TFile::Open(outfile.c_str(), "RECREATE", "", 0);
    if (!filler.tfile)
      throw std::runtime_error("Error opening " + outfile + " for output");
    std::cout << "Running with " << infiles[0] << " -> " << outfile << std::endl;
    filler.tree = new TTree("Events", "Events", 99, filler.tfile);
    filler.tree->Branch("run", &run, "run/s");
    filler.tree->Branch("orbit", &orbit, "orbit/i");
    filler.tree->Branch("bx", &bx, "bx/s");
    filler.tree->Branch("good", &good, "good/O");
    filler.tree->Branch("nPuppi", &nwords, "nPuppi/s");
    filler.tree->Branch("Puppi_pt", &data.pt, "Puppi_pt[nPuppi]/F");
    filler.tree->Branch("Puppi_eta", &data.eta, "Puppi_eta[nPuppi]/F");
    filler.tree->Branch("Puppi_phi", &data.phi, "Puppi_phi[nPuppi]/F");
    filler.tree->Branch("Puppi_pdgId", &data.pdgid, "Puppi_pdgId[nPuppi]/S");
    filler.tree->Branch("Puppi_z0", &data.z0, "Puppi_z0[nPuppi]/F");
    filler.tree->Branch("Puppi_dxy", &data.dxy, "Puppi_dxy[nPuppi]/F");
    filler.tree->Branch("Puppi_wpuppi", &data.wpuppi, "Puppi_wpuppi[nPuppi]/F");
    filler.tree->Branch("Puppi_quality", &data.quality, "Puppi_quality[nPuppi]/b");
    filler.tree->Branch("Triplet_Index", filler.indices, "Triplet_Index[3]/i");
    filler.tree->Branch("Triplet_Mass", &filler.mass, "Triplet_Mass/F");
  }
  // loop
  for (int ifile = 0, nfiles = fins.size(); fins[ifile].good(); ifile = (ifile == nfiles - 1 ? 0 : ifile + 1)) {
    std::fstream &fin = fins[ifile];
    do {
      fin.read(reinterpret_cast<char *>(&header), sizeof(uint64_t));
    } while (header == 0 && fin.good());
    if (!header)
      continue;
    parseHeader(header, run, bx, orbit, good, nwords);
    ntot++;
    if (nwords)
      fin.read(reinterpret_cast<char *>(payload), nwords * sizeof(uint64_t));
    if (analyzeRawEvent(nwords, payload, data, npre, filler)) {
      masshist->Fill(filler.mass);
      npass++;
    }
  }
  if (outformat == "histo") {
    saveHisto(masshist, outfile);
  } else if (outformat == "rawhisto") {
    saveRawHisto(masshist, outfile);
  } else if (outformat == "snapshot") {
    filler.tree->Write();
    filler.tfile->Close();
  }
  double dt = (std::chrono::duration<double>(std::chrono::steady_clock::now() - tstart)).count();
  auto ret = makeReport(dt, ntot, infiles, outfile);
  printf("Run on %d files (%s), %lu events, preselected %lu events (%.4f), selected %lu events (%.6f).\n",
         int(infiles.size()),
         infiles.front().c_str(),
         ntot,
         npre,
         npre / float(ntot),
         npass,
         npass / float(ntot));

  return ret;
}
