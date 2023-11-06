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

bool w3piExample2022Raw::analyzeRawEvent(uint16_t nwords,
                                         uint64_t *payload,
                                         unsigned long int &ntot,
                                         unsigned long int &npre,
                                         unsigned long int &npass,
                                         TreeFiller &filler) const {
  ntot++;
  ROOT::RVec<ULong64_t> raw(reinterpret_cast<ULong64_t *>(payload), nwords);
  ROOT::RVec<float> pts = w3piExample2022::unpackPtFromRaw(raw);
  ROOT::RVec<uint8_t> ids = w3piExample2022::unpackIDFromRaw(raw);
  ROOT::RVec<short> pdgids = w3piExample2022::unpackPID(ids);
  //this eliminates most of single neutrino events
  int lowcut = 0;
  int intermediatecut = 0;
  int highcut = 0;
  for (int i = 0, n = pts.size(); i < n; ++i) {
    if (std::abs(pdgids[i]) == 11 or std::abs(pdgids[i]) == 211) {
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
  ROOT::RVec<float> etas = w3piExample2022::unpackEtaFromRaw(raw);
  ROOT::RVec<float> phis = w3piExample2022::unpackPhiFromRaw(raw);
  ROOT::RVec<ROOT::RVec<unsigned int>> triplets;  //stores all passing triplets (best one selected at the end)
  ROOT::RVec<unsigned int> ix;                    //pion indeces
  ROOT::RVec<int> icharge;                        //pion charges
  ROOT::RVec<float> ptsums;
  ROOT::RVec<unsigned int> iso(pdgids.size(),
                               0);  //stores whether a particle passes isolation test so we don't calculate reliso twice

  for (unsigned int i = 0, n = pdgids.size(); i < n; ++i) {  //make list of all hadrons
    if ((std::abs(pdgids[i]) == 211 or std::abs(pdgids[i]) == 11) and pts[i] >= cuts.minpt1) {
      ix.push_back(i);
      icharge.push_back(abs(pdgids[i]) == 11 ? (pdgids[i] > 0 ? -1 : +1) : (pdgids[i] > 0 ? +1 : -1));
    }
  }

  unsigned int npions = ix.size();
  if (npions > 2) {  //if there are 3+ pions
    float ptsum;

    for (unsigned int i1 = 0; i1 < npions; ++i1) {
      if (pts[ix[i1]] < cuts.minpt3)
        continue;  //high pt cut
      if (isolation(ix[i1], etas, phis, pts, cuts.mindr2, cuts.maxdr2, cuts.maxiso) == 0)
        continue;  //check iso of high pt pion
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
          ROOT::RVec<unsigned int> tr{ix[i1], ix[i2], ix[i3]};  //triplet of indeces

          if (std::abs(icharge[i1] + icharge[i2] + icharge[i3]) == 1) {
            //make Lorentz vectors for each triplet
            auto mass = tripletmass(tr, pts, etas, phis);
            if (mass >= cuts.minmass and mass <= cuts.maxmass) {  //MASS test
              if (deltar(etas[ix[i1]], etas[ix[i3]], phis[ix[i1]], phis[ix[i3]], cuts.mindeltar2) == true and
                  deltar(etas[ix[i2]], etas[ix[i3]], phis[ix[i2]], phis[ix[i3]], cuts.mindeltar2) ==
                      true) {  //ANGULAR SEPARATION test
                //ISOLATION test for lower 2 pions
                bool isop = true;
                for (int j = 1; j < 3; ++j) {
                  if (iso[tr[j]] == 0) {
                    if (isolation(tr[j], etas, phis, pts, cuts.mindr2, cuts.maxdr2, cuts.maxiso) == false) {
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
                  triplets.push_back(tr);
                  ptsum = pts[tr[0]] + pts[tr[1]] + pts[tr[2]];
                  ptsums.push_back(ptsum);
                }  // iso
              }    // delta R
            }      // mass
          }        //charge
        }          //low pt cut
      }            //intermediate pt cut
    }              //high pt cut
  }                //if 3 or more pions

  if (triplets.empty())
    return false;
  float bestscore = ptsums[0];
  unsigned int best = 0;  //index of best triplet in triplet array
  for (unsigned int i = 1, n = triplets.size(); i < n; ++i) {
    float score = ptsums[i];
    if (score > bestscore) {
      bestscore = score;
      best = i;
    }
  }
  npass++;
  std::copy(triplets[best].begin(), triplets[best].end(), filler.indices);
  filler.mass = w3piExample2022::tripletmass(triplets[best], pts, etas, phis);
  if (filler.tree) {
    ROOT::RVec<float> z0s = w3piExample2022::unpackZ0FromRaw(raw, ids);
    ROOT::RVec<float> dxys = w3piExample2022::unpackDxyFromRaw(raw, ids);
    ROOT::RVec<float> wpuppi = w3piExample2022::unpackWPuppiFromRaw(raw, ids);
    ROOT::RVec<uint8_t> quality = w3piExample2022::unpackQualityFromRaw(raw, ids);
    filler.bpt->SetAddress((void *)pts.data());
    filler.beta->SetAddress((void *)etas.data());
    filler.bphi->SetAddress((void *)phis.data());
    filler.bpdgId->SetAddress((void *)pdgids.data());
    filler.bz0->SetAddress((void *)z0s.data());
    filler.bdxy->SetAddress((void *)dxys.data());
    filler.bwpuppi->SetAddress((void *)wpuppi.data());
    filler.bquality->SetAddress((void *)quality.data());
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
  unsigned long int ntot = 0, npre = 0, npass = 0;
  TreeFiller filler;

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
  uint64_t header, payload[1 << 12];

  if (outformat == "snapshot") {
    filler.tfile = TFile::Open(outfile.c_str(), "RECREATE", "", 0);
    filler.tree = new TTree("Events", "Events");
    filler.tree->Branch("run", &run, "run/s");
    filler.tree->Branch("orbit", &orbit, "orbit/i");
    filler.tree->Branch("bx", &bx, "bx/s");
    filler.tree->Branch("good", &good, "good/O");
    filler.tree->Branch("nPuppi", &nwords, "nPuppi/s");
    float dummyf[1];
    short int dummysi[1];
    uint8_t dummyu8[1];
    filler.bpt = filler.tree->Branch("Puppi_pt", dummyf, "Puppi_pt[nPuppi]/F");
    filler.beta = filler.tree->Branch("Puppi_eta", dummyf, "Puppi_eta[nPuppi]/F");
    filler.bphi = filler.tree->Branch("Puppi_phi", dummyf, "Puppi_phi[nPuppi]/F");
    filler.bpdgId = filler.tree->Branch("Puppi_pdgId", dummysi, "Puppi_pdgId[nPuppi]/S");
    filler.bz0 = filler.tree->Branch("Puppi_z0", dummyf, "Puppi_z0[nPuppi]/F");
    filler.bdxy = filler.tree->Branch("Puppi_dxy", dummyf, "Puppi_dxy[nPuppi]/F");
    filler.bwpuppi = filler.tree->Branch("Puppi_wpuppi", dummyf, "Puppi_wpuppi[nPuppi]/F");
    filler.bquality = filler.tree->Branch("Puppi_quality", dummyu8, "Puppi_quality[nPuppi]/b");
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
    if (nwords)
      fin.read(reinterpret_cast<char *>(payload), nwords * sizeof(uint64_t));
    if (analyzeRawEvent(nwords, payload, ntot, npre, npass, filler)) {
      masshist->Fill(filler.mass);
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
