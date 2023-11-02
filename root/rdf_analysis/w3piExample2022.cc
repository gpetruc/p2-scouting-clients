/**
 * Example W -> 3 pi+/- analysis code
 * 
 * Originally developed by Catherine Miller, Boston University, 2022
 * https://github.com/catherine-miller/w3pianalysis/tree/master
 * 
 * Modifications by Giovanni Petrucciani to run on different
 * input files formats, and slight optimization of the code.
 */

#include "w3piExample2022.h"

#include "ROOT/RNTupleDS.hxx"
#include <ROOT/RSnapshotOptions.hxx>
#ifdef USE_ARROW
  #include "RArrowDS2.hxx"
#endif
#include <chrono>

w3piExample2022::w3piExample2022(const std::string &cutChoice, bool verbose) : verbose_(verbose) {
  if (cutChoice == "loose") {
    cuts.minpt1 = 7;   // 9
    cuts.minpt2 = 12;  // 15
    cuts.minpt3 = 15;  // 20
    cuts.mindeltar2 = 0.5 * 0.5;
    cuts.minmass = 40;   // 60
    cuts.maxmass = 150;  // 100
    cuts.mindr2 = 0.01 * 0.01;
    cuts.maxdr2 = 0.25 * 0.25;
    cuts.maxiso = 2.0;  //0.4
  } else if (cutChoice == "tight") {
    cuts.minpt1 = 9;
    cuts.minpt2 = 15;
    cuts.minpt3 = 20;
    cuts.mindeltar2 = 0.5 * 0.5;
    cuts.minmass = 60;
    cuts.maxmass = 100;
    cuts.mindr2 = 0.01 * 0.01;
    cuts.maxdr2 = 0.25 * 0.25;
    cuts.maxiso = 1.0;
  }
}

void w3piExample2022::analyze(ROOT::RDataFrame &top,
                              const std::string &format,
                              unsigned long int &ntot,
                              unsigned long int &npre,
                              unsigned long int &npass,
                              const std::string &outFormat,
                              const std::string &outFile) const {
  //speeds up code to have an initial filter that does not read many branches
  //this eliminates most of single neutrino events
  float minpt1 = cuts.minpt1, minpt2 = cuts.minpt2, minpt3 = cuts.minpt3;
  auto initptcut = [minpt1, minpt2, minpt3](const ROOT::RVec<float> &pts, const ROOT::RVec<short> &pdgids) -> bool {
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
  };

  //SELECTION
  //generates list of pion triplets passing all tests
  auto maketriplets = [&](const ROOT::RVec<short> &pdgids,
                          const ROOT::RVec<float> &pts,
                          const ROOT::RVec<float> &etas,
                          const ROOT::RVec<float> &phis) {
    ROOT::RVec<ROOT::RVec<unsigned int>> triplets;  //stores all passing triplets (best one selected at the end)
    ROOT::RVec<unsigned int> ix;                    //pion indeces
    ROOT::RVec<int> icharge;                        //pion charges
    ROOT::RVec<float> ptsums;
    ROOT::RVec<unsigned int> iso(
        pdgids.size(),
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
  };

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
  ROOT::RDF::RNode d = top;
  bool isInt = (format.length() >= 4 && format.substr(format.length() - 4) == "_int");
  if (format.find("raw64") != std::string::npos) {
    d = d.Define("Puppi_pt", unpackPtFromRaw, {"Puppi_packed"})
            .Define("Puppi_eta", unpackEtaFromRaw, {"Puppi_packed"})
            .Define("Puppi_phi", unpackPhiFromRaw, {"Puppi_packed"})
            .Define("Puppi_pid", unpackIDFromRaw, {"Puppi_packed"})
            .Define("Puppi_pdgId", unpackPID, {"Puppi_pid"})
            .Define("Puppi_dxy", unpackDxyFromRaw, {"Puppi_packed", "Puppi_pid"})
            .Define("Puppi_z0", unpackZ0FromRaw, {"Puppi_packed", "Puppi_pid"})
            .Define("Puppi_wpuppi", unpackWPuppiFromRaw, {"Puppi_packed", "Puppi_pid"})
            .Define("Puppi_quality", unpackQualityFromRaw, {"Puppi_packed", "Puppi_pid"});
    if (format.find("rntuple") != std::string::npos)
      d = d.Alias("nPuppi", "#Puppi_packed");
  } else if (format.find("rntuple_coll") != std::string::npos || format.find("arrow") != std::string::npos) {
    d = d.Alias("nPuppi", "#Puppi")
            .Alias("Puppi_pt", "Puppi.pt")
            .Alias("Puppi_eta", "Puppi.eta")
            .Alias("Puppi_phi", "Puppi.phi")
            .Alias("Puppi_dxy", "Puppi.dxy")
            .Alias("Puppi_z0", "Puppi.z0")
            .Alias(isInt ? "Puppi_pid" : "Puppi_pdgId", isInt ? "Puppi.pid" : "Puppi.pdgId")
            .Alias("Puppi_wpuppi", "Puppi.wpuppi")
            .Alias("Puppi_quality", "Puppi.quality");
  } else if (format == "mc") {
    d = d.Alias("nPuppi", "#Puppi")
            .Alias("Puppi_pt", "L1Puppi_pt")
            .Alias("Puppi_eta", "L1Puppi_eta")
            .Alias("Puppi_phi", "L1Puppi_phi")
            .Alias("Puppi_dxy", "L1Puppi_dxy")
            .Alias("Puppi_z0", "L1Puppi_z0")
            .Define("Puppi_pdgId", convertIds, {"L1Puppi_pdgId"})
            .Alias("Puppi_wpuppi", "L1Puppi_wpuppi")
            .Alias("Puppi_quality", "L1Puppi_quality");
  }
  if (isInt) {
    d = d.Redefine("Puppi_pt", unpackPt, {"Puppi_pt"})
            .Redefine("Puppi_eta", unpackEtaPhi, {"Puppi_eta"})
            .Redefine("Puppi_phi", unpackEtaPhi, {"Puppi_phi"})
            .Define("Puppi_pdgId", unpackPID, {"Puppi_pid"})
            .Redefine("Puppi_dxy", unpackDxy, {"Puppi_dxy"})
            .Redefine("Puppi_z0", unpackZ0, {"Puppi_z0"})
            .Redefine("Puppi_wpuppi", unpackWPuppi, {"Puppi_wpuppi"});
  }
  auto c0 = d.Count();
  auto d1 = d.Filter(initptcut, {"Puppi_pt", "Puppi_pdgId"});
  auto c1 = d1.Count();
  auto d2 = d1.Define("Triplet_Index", maketriplets, {"Puppi_pdgId", "Puppi_pt", "Puppi_eta", "Puppi_phi"})
                .Filter(notempty, {"Triplet_Index"})
                .Define("Triplet_Mass", tripletmass, {"Triplet_Index", "Puppi_pt", "Puppi_eta", "Puppi_phi"});
  auto masshist =
      d2.Histo1D<float>({"masshist", "W Boson mass from selected pions; mass (GeV/c^2)", 100, 0, 100}, "Triplet_Mass");

  if (outFormat == "snapshot") {
    ROOT::RDF::RSnapshotOptions opts;
    opts.fCompressionLevel = 0;
    d2.Snapshot<uint16_t,              // run
                uint32_t,              // orbit
                uint16_t,              // bx
                bool,                  // good
                uint16_t,              // nPuppi
                ROOT::RVec<float>,     // Puppi_pt
                ROOT::RVec<float>,     // Puppi_eta
                ROOT::RVec<float>,     // Puppi_phi
                ROOT::RVec<int16_t>,   // Puppi_pdgId
                ROOT::RVec<float>,     // Puppi_z0
                ROOT::RVec<float>,     // Puppi_dxy
                ROOT::RVec<float>,     // Puppi_wpuppi
                ROOT::RVec<uint8_t>,   // Puppi_quality
                ROOT::RVec<unsigned>,  // Triplet_Index
                float                  // Triplet_mass
                >("Events", outFile.c_str(), outputs, opts);
  }
  ntot = *c0;
  npre = *c1;
  npass = masshist->GetEntries();
  if (outFormat == "histo") {
    saveHisto(masshist.GetPtr(), outFile);
  } else if (outFormat == "rawhisto") {
    saveRawHisto(masshist.GetPtr(), outFile);
  }
}

rdfAnalysis::Report w3piExample2022::run(const std::string &format,
                                         const std::vector<std::string> &infiles,
                                         const std::string &outformat,
                                         const std::string &outfile) const {
  auto tstart = std::chrono::steady_clock::now();
  //increase verbosity to see how long this is taking
  auto verbosity = ROOT::Experimental::RLogScopedVerbosity(
      ROOT::Detail::RDF::RDFLogChannel(),
      verbose_ ? ROOT::Experimental::ELogLevel::kInfo : ROOT::Experimental::ELogLevel::kWarning);
  // and suppress RNTuple verbosity
  auto rntVerbosity =
      ROOT::Experimental::RLogScopedVerbosity(ROOT::Experimental::NTupleLog(), ROOT::Experimental::ELogLevel::kError);

  unsigned long int ntot, npre, npass;
  if (format.find("rntuple") == 0) {
    assert(infiles.size() == 1);
    ROOT::RDataFrame d = ROOT::RDF::Experimental::FromRNTuple("Events", infiles.front());
    //d.Describe().Print();
    analyze(d, format, ntot, npre, npass, outformat, outfile);
#ifdef USE_ARROW
  } else if (format.find("arrow_file") == 0) {
    assert(infiles.size() == 1);
    ROOT::RDataFrame d = ROOT::RDF::FromArrowIPCFile(infiles.front(), {});
    //d.Describe().Print();
    analyze(d, format, ntot, npre, npass, outformat, outfile);
  } else if (format.find("arrow_stream") == 0) {
    assert(infiles.size() == 1);
    ROOT::RDataFrame d = ROOT::RDF::FromArrowIPCStream(infiles.front(), {});
    //d.Describe().Print();
    analyze(d, format, ntot, npre, npass, outformat, outfile);
#endif
  } else {
    ROOT::RDataFrame d("Events", infiles);
    //d.Describe().Print();
    analyze(d, format, ntot, npre, npass, outformat, outfile);
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
