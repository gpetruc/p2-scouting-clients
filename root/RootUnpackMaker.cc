#include "RootUnpackMaker.h"

#include "RNTupleUnpackerBase.h"
#include "RNTupleUnpackerFloats.h"
#include "RNTupleUnpackerCollFloat.h"
#include "RNTupleUnpackerInts.h"
#include "RNTupleUnpackerCollInt.h"
#include "RNTupleUnpackerRaw64.h"
#include "GMTTkMuRNTupleUnpackerFloats.h"
#include "GMTTkMuRNTupleUnpackerCollFloat.h"
#include "TTreeUnpackerBase.h"
#include "TTreeUnpackerFloats.h"
#include "TTreeUnpackerInts.h"
#include "TTreeUnpackerRaw64.h"
#include "GMTTkMuTTreeUnpackerFloats.h"
#include "GMTTkMuTTreeUnpackerInts.h"
#include <stdexcept>

RootUnpackMaker::Spec::Spec(const std::string &obj,   // puppi or tkmu
                            const std::string &kind,  // ttree or rntuple
                            const std::string &unpackFormat,
                            const std::string &compression,
                            int level)
    : format(unpackFormat), compressionAlgo(compression), compressionLevel(level) {
  if (obj == "puppi") {
    objType = ObjType::Puppi;
  } else if (obj == "tkmu") {
    objType = ObjType::TkMu;
  } else {
    throw std::invalid_argument("Unsupported object type '" + obj + "', must be 'puppi' or 'tkmu'");
  }
  if (kind == "ttree") {
    fileKind = FileKind::TTree;
  } else if (kind == "rntuple") {
    fileKind = FileKind::RNTuple;
  } else {
    throw std::invalid_argument("Unsupported file kind '" + kind + "', must be 'ttree' or 'rntuple'");
  }
}
std::unique_ptr<UnpackerBase> RootUnpackMaker::make(const RootUnpackMaker::Spec &spec) {
  std::unique_ptr<UnpackerBase> unpacker;
  switch (spec.objType) {
    case Spec::ObjType::Puppi:
      switch (spec.fileKind) {
        case Spec::FileKind::TTree:
          if (spec.format == "float" || spec.format == "float24") {
            unpacker = std::make_unique<TTreeUnpackerFloats>(spec.format);
          } else if (spec.format == "int") {
            unpacker = std::make_unique<TTreeUnpackerInts>();
          } else if (spec.format == "raw64") {
            unpacker = std::make_unique<TTreeUnpackerRaw64>();
          } else {
            throw std::invalid_argument("Unsupported puppi ttree format " + spec.format);
          }
          break;
        case Spec::FileKind::RNTuple:
          if (spec.format == "floats") {
            unpacker = std::make_unique<RNTupleUnpackerFloats>();
          } else if (spec.format == "coll_float") {
            unpacker = std::make_unique<RNTupleUnpackerCollFloat>();
          } else if (spec.format == "ints") {
            unpacker = std::make_unique<RNTupleUnpackerInts>();
          } else if (spec.format == "coll_int") {
            unpacker = std::make_unique<RNTupleUnpackerCollInt>();
          } else if (spec.format == "raw64") {
            unpacker = std::make_unique<RNTupleUnpackerRaw64>();
          } else {
            throw std::invalid_argument("Unsupported puppi rntuple format " + spec.format);
          }
          break;
      }
      break;
    case Spec::ObjType::TkMu:
      switch (spec.fileKind) {
        case Spec::FileKind::TTree:
          if (spec.format == "float" || spec.format == "float24") {
            unpacker = std::make_unique<GMTTkMuTTreeUnpackerFloats>(spec.format);
          } else if (spec.format == "int") {
            unpacker = std::make_unique<GMTTkMuTTreeUnpackerInts>();
          } else {
            throw std::invalid_argument("Unsupported tkmu ttree format " + spec.format);
          }
          break;
        case Spec::FileKind::RNTuple:
          if (spec.format == "floats") {
            unpacker = std::make_unique<GMTTkMuRNTupleUnpackerFloats>();
          } else if (spec.format == "coll_float") {
            unpacker = std::make_unique<GMTTkMuRNTupleUnpackerCollFloat>();
          } else {
            throw std::invalid_argument("Unsupported tkmu rntuple format " + spec.format);
          }
          break;
      }
  }

  if (unpacker)
    unpacker->setCompression(spec.compressionAlgo, spec.compressionLevel);
  return unpacker;
}