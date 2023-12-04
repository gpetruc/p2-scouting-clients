#ifndef p2_clients_root_rdf_analysis_w3piExample2022Raw_h
#define p2_clients_root_rdf_analysis_w3piExample2022Raw_h

#include "w3piExample2022.h"
#include <fstream>

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
    unsigned int indices[3];
    float mass;
    TreeFiller() : tree(nullptr) {}
    std::fstream rawfile;
  };
  struct Data {
    //puppi candidate info:
    float pt[255];
    float eta[255], phi[255];
    short int pdgid[255];
    uint8_t quality[255];
    //charged only:
    float z0[255];
    float dxy[255];
    //neutral only:
    float wpuppi[255];
  };
  bool analyzeRawEvent(
      uint16_t nwords, const ULong64_t *payload, Data &data, unsigned long int &npre, TreeFiller &filler) const;
};

#endif
