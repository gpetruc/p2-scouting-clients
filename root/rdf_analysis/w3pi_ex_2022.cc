/**
 * Example W -> 3 pi+/- analysis code
 * 
 * Originally developed by Catherine Miller, Boston University, 2022
 * https://github.com/catherine-miller/w3pianalysis/tree/master
 * 
 * Modifications by Giovanni Petrucciani to run on different
 * input files formats, and slight optimization of the code.
 */

#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <vector>

#include "Math/Vector4D.h"
#include "ROOT/RDataFrame.hxx"
#include "ROOT/RNTupleDS.hxx"
#include "ROOT/RVec.hxx"
#include "TFile.h"
#include "TH1D.h"
#include <Math/GenVector/LorentzVector.h>
#include <Math/GenVector/PtEtaPhiM4D.h>
#include <ROOT/RLogger.hxx>
#include <ROOT/RSnapshotOptions.hxx>
#include <TStopwatch.h>

#include <getopt.h>

template <typename T>
using Vec = ROOT::RVec<T>;
using ROOT::Math::PtEtaPhiMVector;
using ROOT::Math::XYZTVector;

// cut values and other parameters
// looser cuts implemented to increase the efficiency of the background
// to simulate a possible skimming to be used online.
// the tighter cuts for the analysis study by Catherine are in the comments
float minpt1 = 7;   // 9
float minpt2 = 12;  // 15
float minpt3 = 15;  // 20
float mindeltar2 = 0.5 * 0.5;
float minmass = 40;   // 60
float maxmass = 150;  // 100
float mindr2 = 0.01 * 0.01;
float maxdr2 = 0.25 * 0.25;
float maxiso = 2.0;  //0.4

//TEST functions
bool isolation(unsigned int pidex,
               const Vec<float> &eta,
               const Vec<float> &phi,
               const Vec<float> &pt,
               float mindr2,
               float maxdr2,
               float maxiso) {
  bool passed = false;
  float psum = 0;
  for (unsigned int j = 0u, n = pt.size(); j < n; ++j) {  //loop over other particles
    if (pidex == j)
      continue;
    float deta = eta[pidex] - eta[j], dphi = ROOT::VecOps::DeltaPhi<float>(phi[pidex], phi[j]);
    float dr2 = deta * deta + dphi * dphi;
    if (dr2 >= mindr2 && dr2 <= maxdr2)
      psum += pt[j];
  }
  if (psum / pt[pidex] <= maxiso)
    passed = true;
  return passed;
}

bool deltar(float eta1, float eta2, float phi1, float phi2, float mindeltar2) {
  bool passed = true;
  float deta = eta1 - eta2;
  float dphi = ROOT::VecOps::DeltaPhi<float>(phi1, phi2);
  float dr2 = deta * deta + dphi * dphi;
  if (dr2 < mindeltar2) {
    passed = false;
    return passed;
  }
  return passed;
}

//speeds up code to have an initial filter that does not read many branches
//this eliminates most of single neutrino events
bool initptcut(const Vec<float> &pts, const Vec<short> &pdgids) {
  int lowcut = 0;
  int intermediatecut = 0;
  int highcut = 0;
  for (int i = 0, n = pts.size(); i < n; ++i) {
    if (std::abs(pdgids[i]) == 11 or std::abs(pdgids[i]) == 211) {
      if (pts[i] >= minpt1) {
        lowcut++;
        if (pts[i] >= minpt2) {
          intermediatecut++;
          if (pts[i] >= minpt3)
            highcut++;
        }
      }
    }
  }
  return (lowcut > 2 and intermediatecut > 1 and highcut > 0);
}

//SELECTION
//generates list of pion triplets passing all tests
auto maketriplets(const Vec<short> &pdgids, const Vec<float> &pts, const Vec<float> &etas, const Vec<float> &phis) {
  Vec<Vec<unsigned int>> triplets;  //stores all passing triplets (best one selected at the end)
  Vec<unsigned int> ix;             //pion indeces
  Vec<int> icharge;                 //pion charges
  Vec<float> ptsums;
  Vec<unsigned int> iso(pdgids.size(),
                        0);  //stores whether a particle passes isolation test so we don't calculate reliso twice

  for (unsigned int i = 0, n = pdgids.size(); i < n; ++i) {  //make list of all hadrons
    if ((std::abs(pdgids[i]) == 211 or std::abs(pdgids[i]) == 11) and pts[i] >= minpt1) {
      ix.push_back(i);
      icharge.push_back(abs(pdgids[i]) == 11 ? (pdgids[i] > 0 ? -1 : +1) : (pdgids[i] > 0 ? +1 : -1));
    }
  }

  unsigned int npions = ix.size();
  if (npions > 2) {  //if there are 3+ pions
    float ptsum;

    for (unsigned int i1 = 0; i1 < npions; ++i1) {
      if (pts[ix[i1]] < minpt3)
        continue;  //high pt cut
      if (isolation(ix[i1], etas, phis, pts, mindr2, maxdr2, maxiso) == 0)
        continue;  //check iso of high pt pion
      for (unsigned int i2 = 0; i2 < npions; ++i2) {
        if (i2 == i1 || pts[ix[i2]] < minpt2)
          continue;
        if (pts[ix[i2]] > pts[ix[i1]] || (pts[ix[i2]] == pts[ix[i1]] and i2 < i1))
          continue;  //intermediate pt cut
        if (deltar(etas[ix[i1]], etas[ix[i2]], phis[ix[i1]], phis[ix[i2]], mindeltar2) == false)
          continue;  //angular sep of top 2 pions
        for (unsigned int i3 = 0; i3 < npions; ++i3) {
          if (i3 == i1 or i3 == i2)
            continue;
          if (pts[ix[i2]] < minpt1)
            continue;  //low pt cut
          if (pts[ix[i3]] > pts[ix[i1]] || (pts[ix[i3]] == pts[ix[i1]] and i3 < i1))
            continue;
          if (pts[ix[i3]] > pts[ix[i2]] || (pts[ix[i3]] == pts[ix[i2]] and i3 < i2))
            continue;
          Vec<unsigned int> tr{ix[i1], ix[i2], ix[i3]};  //triplet of indeces

          if (std::abs(icharge[i1] + icharge[i2] + icharge[i3]) == 1) {
            //make Lorentz vectors for each triplet
            ROOT::Math::PtEtaPhiMVector p1(pts[tr[0]], etas[tr[0]], phis[tr[0]], 0.1396);
            ROOT::Math::PtEtaPhiMVector p2(pts[tr[1]], etas[tr[1]], phis[tr[1]], 0.1396);
            ROOT::Math::PtEtaPhiMVector p3(pts[tr[2]], etas[tr[2]], phis[tr[2]], 0.1396);
            auto mass = (p1 + p2 + p3).M();
            if (mass >= minmass and mass <= maxmass) {  //MASS test
              if (deltar(etas[ix[i1]], etas[ix[i3]], phis[ix[i1]], phis[ix[i3]], mindeltar2) == true and
                  deltar(etas[ix[i2]], etas[ix[i3]], phis[ix[i2]], phis[ix[i3]], mindeltar2) ==
                      true) {  //ANGULAR SEPARATION test
                //ISOLATION test for lower 2 pions
                bool isop = true;
                for (int j = 1; j < 3; ++j) {
                  if (iso[tr[j]] == 0) {
                    if (isolation(tr[j], etas, phis, pts, mindr2, maxdr2, maxiso) == false) {
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
    triplets.emplace_back();
  if (triplets.size() == 1)
    return triplets[0];

  //if there are multiple triplets passing, choose the best
  float bestscore = 0;
  unsigned int best = 0;  //index of best triplet in triplet array
  for (unsigned int i = 0, n = triplets.size(); i < n; ++i) {
    float score = ptsums[i];
    if (score > bestscore) {
      bestscore = score;
      best = i;
    }
  }
  return triplets[best];
}

//processing after selection: calculate mass and check the triplet isn't empty

bool notempty(const Vec<unsigned int> &index) {  //used to check if any triplets passed in an event
  return !index.empty();
}

auto tripletmass(const Vec<unsigned int> &t, const Vec<float> &pts, const Vec<float> &etas, const Vec<float> &phis) {
  ROOT::Math::PtEtaPhiMVector p1(pts[t[0]], etas[t[0]], phis[t[0]], 0.1396);
  ROOT::Math::PtEtaPhiMVector p2(pts[t[1]], etas[t[1]], phis[t[1]], 0.1396);
  ROOT::Math::PtEtaPhiMVector p3(pts[t[2]], etas[t[2]], phis[t[2]], 0.1396);
  float mass = (p1 + p2 + p3).M();
  return mass;
}

auto convertIds(const Vec<int> &mcids) { return ROOT::VecOps::Construct<short>(mcids); }

void analyze(ROOT::RDataFrame &d,
             const std::string &format,
             unsigned int &ntot,
             unsigned int &npre,
             unsigned int &npass,
             const std::string &output,
             const char *fout) {
  std::string c_pt, c_eta, c_phi, c_pdgId;
  std::vector<std::string> outputs = {"run",
                                      "orbit",
                                      "bx",
                                      "good",
                                      "nPuppi",
                                      "Puppi_pt",
                                      "Puppi_eta",
                                      "Puppi_phi",
                                      "Puppi_pdgId",
                                      "Puppi_z0",
                                      "Puppi_dxy",
                                      "Puppi_wpuppi",
                                      "Puppi_quality",
                                      "Triplet_Index",
                                      "Triplet_Mass"};
  bool isMC = false;
  if (format == "tree" || format == "rntuple_vec") {
    c_pt = "Puppi_pt";
    c_eta = "Puppi_eta";
    c_phi = "Puppi_phi";
    c_pdgId = "Puppi_pdgId";
  } else if (format == "mc") {
    isMC = true;
    c_pt = "Puppi_pt";
    c_eta = "L1Puppi_eta";
    c_phi = "L1Puppi_phi";
    c_pdgId = "Puppi_pdgId";
  } else if (format == "rntuple_coll") {
    c_pt = "Puppi.pt";
    c_eta = "Puppi.eta";
    c_phi = "Puppi.phi";
    c_pdgId = "Puppi.pdgId";
    outputs = {"run",
               "orbit",
               "bx",
               "good",
               "nPuppi",
               "Puppi.pt",
               "Puppi.eta",
               "Puppi.phi",
               "Puppi.pdgId",
               "Puppi.z0",
               "Puppi.dxy",
               "Puppi.wpuppi",
               "Puppi.quality",
               "Triplet_Index",
               "Triplet_Mass"};
  }
  auto c0 = d.Count();
  auto d1 = isMC ? d.Define("Puppi_pdgId", convertIds, {"L1Puppi_pdgId"}).Filter(initptcut, {c_pt, c_pdgId})
                 : d.Filter(initptcut, {c_pt, c_pdgId});
  auto c1 = d1.Count();
  auto d2 = d1.Define("Triplet_Index", maketriplets, {c_pdgId, c_pt, c_eta, c_phi})
                .Filter(notempty, {"Triplet_Index"})
                .Define("Triplet_Mass", tripletmass, {"Triplet_Index", c_pt, c_eta, c_phi});
  auto masshist =
      d2.Histo1D<float>({"masshist", "W Boson mass from selected pions; mass (GeV/c^2)", 100, 0, 100}, "Triplet_Mass");

  if (output == "snapshot") {
    ROOT::RDF::RSnapshotOptions opts;
    opts.fCompressionLevel = 0;
    if (format == "rntuple_coll") {
      auto d3 = d2.Alias("nPuppi", "#Puppi");
      d3.Snapshot<uint16_t,              // run
                  uint32_t,              // orbit
                  uint16_t,              // bx
                  bool,                  // good
                  uint16_t,              // nPuppi
                  ROOT::RVec<float>,     // Puppi_pt (RNTuple reads them as RVec)
                  ROOT::RVec<float>,     // Puppi_eta
                  ROOT::RVec<float>,     // Puppi_phi
                  ROOT::RVec<int16_t>,   // Puppi_pdgId
                  ROOT::RVec<float>,     // Puppi_z0
                  ROOT::RVec<float>,     // Puppi_dxy
                  ROOT::RVec<float>,     // Puppi_wpuppi
                  ROOT::RVec<uint8_t>,   // Puppi_quality
                  ROOT::RVec<unsigned>,  // Triplet_Index
                  float                  // Triplet_mass
                  >("Events", fout, outputs, opts);
    } else {
      d2.Snapshot<uint16_t,              // run
                  uint32_t,              // orbit
                  uint16_t,              // bx
                  bool,                  // good
                  uint16_t,              // nPuppi
                  ROOT::RVec<float>,     // Puppi_pt   (actually a float[], but RDF Snapshot doesn't like it;
                  ROOT::RVec<float>,     // Puppi_eta   JIT-ed Snapshot would get the arrays correctly, though)
                  ROOT::RVec<float>,     // Puppi_phi
                  ROOT::RVec<int16_t>,   // Puppi_pdgId
                  ROOT::RVec<float>,     // Puppi_z0
                  ROOT::RVec<float>,     // Puppi_dxy
                  ROOT::RVec<float>,     // Puppi_wpuppi
                  ROOT::RVec<uint8_t>,   // Puppi_quality
                  ROOT::RVec<unsigned>,  // Triplet_Index
                  float                  // Triplet_mass
                  >("Events", fout, outputs, opts);
    }
  }
  ntot = *c0;
  npre = *c1;
  npass = masshist->GetEntries();
  if (output == "histo") {
    TFile *fOut = TFile::Open(fout, "RECREATE", 0, 0);
    fOut->WriteTObject(masshist->Clone());
    fOut->Close();
  } else if (output == "rawhisto") {
    std::fstream f(fout, std::ios::binary | std::ios::out | std::ios::trunc);
    uint32_t nbins = masshist->GetNbinsX();
    f.write(reinterpret_cast<char *>(&nbins), sizeof(uint32_t));
    f.write(reinterpret_cast<char *>(masshist->GetArray()), (nbins + 2) * sizeof(Double_t));
    f.close();
  }
}

void usage() {
  printf("Usage: w3pi_ex_2022.exe [options] infile [outfile]\n");
  printf("  -f format: tree (default), mc, rntuple_(coll|vec)\n");
  printf("  -o output: none (default), histo, rawhisto, snapshot\n");
  printf("  -v       : verbose printout\n");
  printf("  -j N     : multithread with N threads\n");
}

int main(int argc, char **argv) {
  if (argc <= 1) {
    usage();
    return 1;
  }

  int threads = -1;
  bool verbose = false;
  std::string format = "tree", output = "none";
  while (1) {
    static struct option long_options[] = {{"help", no_argument, nullptr, 'h'},
                                           {"verbose", no_argument, nullptr, 'v'},
                                           {"format", required_argument, nullptr, 'f'},
                                           {"output", required_argument, nullptr, 'o'},
                                           {"threads", required_argument, nullptr, 'j'},
                                           {nullptr, 0, nullptr, 0}};
    int option_index = 0;
    int optc = getopt_long(argc, argv, "hvj:f:o:", long_options, &option_index);
    if (optc == -1)
      break;

    switch (optc) {
      case 'h':
        usage();
        return 0;
      case 'f':
        format = std::string(optarg);
        break;
      case 'j':
        threads = std::atoi(optarg);
        break;
      case 'o':
        output = std::string(optarg);
        break;
      case 'v':
        verbose = true;
        break;
      default:
        usage();
        return 1;
    }
  }

  int iarg = optind, narg = argc - optind;

  if ((output != "none") && (narg == 1)) {
    printf("Error: specifying an output format, you must specify an output filename.\n");
    return 1;
  }

  if (threads > -1)
    ROOT::EnableImplicitMT(threads);

  //increase verbosity to see how long this is taking
  auto verbosity =
      ROOT::Experimental::RLogScopedVerbosity(ROOT::Detail::RDF::RDFLogChannel(), ROOT::Experimental::ELogLevel::kInfo);
  // and suppress RNTuple verbosity
  auto rntVerbosity =
      ROOT::Experimental::RLogScopedVerbosity(ROOT::Experimental::NTupleLog(), ROOT::Experimental::ELogLevel::kError);

  TStopwatch timer;
  unsigned int ntot, npre, npass;
  if (format.find("rntuple") == 0) {
    ROOT::RDataFrame d = ROOT::RDF::Experimental::FromRNTuple("Events", argv[iarg]);
    if (verbose)
      d.Describe().Print();
    analyze(d, format, ntot, npre, npass, output, (narg > 1 ? argv[iarg + 1] : ""));
  } else {
    ROOT::RDataFrame d("Events", argv[iarg]);
    if (verbose)
      d.Describe().Print();
    analyze(d, format, ntot, npre, npass, output, (narg > 1 ? argv[iarg + 1] : ""));
  }

  timer.Stop();
  printf(
      "Run on %s, %9u events, preselected %8u events, selected %8u events (%.6f) in %7.3f s (%7.1f kHz, 40 MHz / "
      "%.1f)\n",
      argv[iarg],
      ntot,
      npre,
      npass,
      npass / float(ntot),
      timer.RealTime(),
      ntot * .001 / timer.RealTime(),
      40e6 * timer.RealTime() / ntot);
  return 0;
}
