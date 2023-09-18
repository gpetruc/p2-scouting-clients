#ifndef p2_clients_root_rdf_analysis_w3piExample2022_h
#define p2_clients_root_rdf_analysis_w3piExample2022_h

#include "analysis.h"
#include <cmath>
#include <ROOT/RDataFrame.hxx>
#include <ROOT/RVec.hxx>
#include <Math/Vector4D.h>
#include <Math/GenVector/LorentzVector.h>
#include <Math/GenVector/PtEtaPhiM4D.h>

class w3piExample2022 : public rdfAnalysis {
public:
  w3piExample2022(const std::string &cutChoice, bool verbose);
  ~w3piExample2022() override {}
  void run(const std::string &informat,
           const std::vector<std::string> &infiles,
           const std::string &outformat,
           const std::string &outfile) const override;

private:
  struct Cuts {
    float minpt1 = 7;   // 9
    float minpt2 = 12;  // 15
    float minpt3 = 15;  // 20
    float mindeltar2 = 0.5 * 0.5;
    float minmass = 40;   // 60
    float maxmass = 150;  // 100
    float mindr2 = 0.01 * 0.01;
    float maxdr2 = 0.25 * 0.25;
    float maxiso = 2.0;  //0.4
  };

private:
  Cuts cuts;
  bool verbose_;

  void analyze(ROOT::RDataFrame &d,
               const std::string &format,
               unsigned int &ntot,
               unsigned int &npre,
               unsigned int &npass,
               const std::string &outputformat,
               const std::string &outFile) const;

  //TEST functions
  inline static bool isolation(unsigned int pidex,
                               const ROOT::RVec<float> &eta,
                               const ROOT::RVec<float> &phi,
                               const ROOT::RVec<float> &pt,
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

  inline static bool deltar(float eta1, float eta2, float phi1, float phi2, float mindeltar2) {
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

  inline static bool notempty(
      const ROOT::RVec<unsigned int> &index) {  //used to check if any triplets passed in an event
    return !index.empty();
  }

  inline static float tripletmass(const ROOT::RVec<unsigned int> &t,
                                  const ROOT::RVec<float> &pts,
                                  const ROOT::RVec<float> &etas,
                                  const ROOT::RVec<float> &phis) {
    ROOT::Math::PtEtaPhiMVector p1(pts[t[0]], etas[t[0]], phis[t[0]], 0.1396);
    ROOT::Math::PtEtaPhiMVector p2(pts[t[1]], etas[t[1]], phis[t[1]], 0.1396);
    ROOT::Math::PtEtaPhiMVector p3(pts[t[2]], etas[t[2]], phis[t[2]], 0.1396);
    float mass = (p1 + p2 + p3).M();
    return mass;
  }

  inline static ROOT::RVec<short> convertIds(const ROOT::RVec<int> &mcids) {
    return ROOT::VecOps::Construct<short>(mcids);
  }
  inline static ROOT::RVec<float> unpackPt(const ROOT::RVec<uint16_t> &ipt) {
    ROOT::RVec<float> ret(ipt.size());
    for (int i = 0, n = ret.size(); i < n; ++i) {
      ret[i] = ipt[i] * 0.25f;
    }
    return ret;
  }
  inline static ROOT::RVec<float> unpackEtaPhi(const ROOT::RVec<int16_t> &etaphi) {
    ROOT::RVec<float> ret(etaphi.size());
    for (int i = 0, n = ret.size(); i < n; ++i) {
      ret[i] = etaphi[i] * float(M_PI / 720.);
    }
    return ret;
  }
  inline static ROOT::RVec<short> unpackPID(const ROOT::RVec<uint8_t> &pids) {
    static constexpr int16_t PDGIDS[8] = {130, 22, -211, 211, 11, -11, 13, -13};
    ROOT::RVec<short> ret(pids.size());
    for (int i = 0, n = ret.size(); i < n; ++i) {
      ret[i] = PDGIDS[pids[i]];
    }
    return ret;
  }
  inline static ROOT::RVec<float> unpackDxy(const ROOT::RVec<int8_t> &dxy) {
    ROOT::RVec<float> ret(dxy.size());
    for (int i = 0, n = ret.size(); i < n; ++i) {
      ret[i] = dxy[i] * 0.05f;  // placeholder
    }
    return ret;
  }
  inline static ROOT::RVec<float> unpackZ0(const ROOT::RVec<int16_t> &z0) {
    ROOT::RVec<float> ret(z0.size());
    for (int i = 0, n = ret.size(); i < n; ++i) {
      ret[i] = z0[i] * 0.05f;
    }
    return ret;
  }
  inline static ROOT::RVec<float> unpackWPuppi(const ROOT::RVec<uint16_t> &wpuppi) {
    ROOT::RVec<float> ret(wpuppi.size());
    for (int i = 0, n = ret.size(); i < n; ++i) {
      ret[i] = wpuppi[i] * float(1 / 256.f);
    }
    return ret;
  }
};

#endif
