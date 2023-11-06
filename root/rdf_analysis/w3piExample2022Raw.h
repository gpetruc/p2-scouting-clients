#ifndef p2_clients_root_rdf_analysis_w3piExample2022Raw_h
#define p2_clients_root_rdf_analysis_w3piExample2022Raw_h

#include "w3piExample2022.h"

class w3piExample2022Raw : public w3piExample2022 {
public:
  w3piExample2022Raw(const std::string &cutChoice, bool verbose);
  ~w3piExample2022Raw() override {}
  Report run(const std::string &informat,
             const std::vector<std::string> &infiles,
             const std::string &outformat,
             const std::string &outfile) const override;

protected:
  struct TreeFiller {
    TFile *tfile;
    TTree *tree;
    TBranch *bpt, *beta, *bphi, *bpdgId, *bz0, *bdxy, *bwpuppi, *bquality;
    unsigned int indices[3];
    float mass;
    TreeFiller() : tree(nullptr) {}
  };
  bool analyzeRawEvent(uint16_t nwords,
                       uint64_t *payload,
                       unsigned long int &ntot,
                       unsigned long int &npre,
                       unsigned long int &npass,
                       TreeFiller &filler) const;
};

#endif
