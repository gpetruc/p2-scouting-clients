#include "ApacheUnpackMaker.h"

#include "ArrowUnpackerBase.h"
#include "ArrowUnpackerFloats.h"
#include "ArrowUnpackerInts.h"
#include "ArrowUnpackerRaw64.h"
#include "ArrowUnpackerTkMuFloats.h"

ApacheUnpackMaker::Spec::Spec(const std::string &obj,
                              const std::string &kind,
                              const std::string &unpackFormat,
                              const std::string &compression,
                              int level)
    : format(unpackFormat), compressionAlgo(compression), compressionLevel(level) {
  if (obj == "puppi") {
    objType = ObjType::Puppi;
  } else if (obj == "tkmu") {
    objType = ObjType::Puppi;
  } else {
    throw std::invalid_argument("Unsupported object type '" + obj + "', must be 'puppi' or 'tkmu'");
  }
  if (kind == "ipcstream") {
    fileKind = FileKind::IPCStream;
  } else if (kind == "ipcfile") {
    fileKind = FileKind::IPCFile;
  } else if (kind == "parquet") {
#ifdef USE_PARQUET
    fileKind = FileKind::Parquet;
#else
    throw std::invalid_argument("Support for Parquet was not compiled into this version of the unpacker");
#endif    
  } else {
    throw std::invalid_argument("Unsupported file kind '" + kind + "', must be 'ipc' or 'parquet'");
  }
}
std::unique_ptr<UnpackerBase> ApacheUnpackMaker::make(const ApacheUnpackMaker::Spec &spec, unsigned int batchsize) {
  std::unique_ptr<UnpackerBase> unpacker;
  switch (spec.objType) {
    case Spec::ObjType::Puppi:
      if (spec.fileKind == Spec::FileKind::IPCStream || spec.fileKind == Spec::FileKind::IPCFile || spec.fileKind == Spec::FileKind::Parquet) {
        if (spec.format == "float" || spec.format == "float16") {
          unpacker = std::make_unique<ArrowUnpackerFloats>(batchsize, spec.fileKind, spec.format == "float16");
        } else if (spec.format == "int") {
          unpacker = std::make_unique<ArrowUnpackerInts>(batchsize, spec.fileKind);
        } else if (spec.format == "raw64") {
          unpacker = std::make_unique<ArrowUnpackerRaw64>(batchsize, spec.fileKind);
        } else {
          throw std::invalid_argument("Unsupported puppi format " + spec.format);
        }
      } else {
        throw std::invalid_argument("Unsupported puppi fileKind " + spec.format);
      }
      break;
    case Spec::ObjType::TkMu:
      if (spec.fileKind == Spec::FileKind::IPCStream || spec.fileKind == Spec::FileKind::IPCFile || spec.fileKind == Spec::FileKind::Parquet) {
        if (spec.format == "float" || spec.format == "float16") {
          unpacker = std::make_unique<ArrowUnpackerTkMuFloats>(batchsize, spec.fileKind, spec.format == "float16");
        } else {
          throw std::invalid_argument("Unsupported tkmu ipc format " + spec.format);
        }
      } else {
        throw std::invalid_argument("Unsupported puppi fileKind " + spec.format);
      }
      break;
  }
  if (unpacker)
    unpacker->setCompression(spec.compressionAlgo, spec.compressionLevel);
  return unpacker;
}