#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/pretty_print.h>
#include <arrow/status.h>
#include <arrow/result.h>
#include <arrow/array.h>
#include <arrow/type.h>
#include <arrow/table.h>

#include <iostream>
#include <cstdio>
#include <chrono>
#include <string>

#include <getopt.h>

using arrow::Status;

void usage() {
  printf("Usage: arrow_read.exe [options] <format> infile.arrow \n");
  printf("   layout := ipcstream | ipcfile \n");
  printf("Options:\n");
  printf("  -j, --cputhreads N: set the capacity of the global CPU thread pool\n");
  printf("  -J, --iothreads N: set the capacity of the global I/O thread pool\n");
}

struct Col {
  std::string name;
  std::string type;
  short int depth;
  Col(const std::string &aname, const std::string &atype, short int adepth = 0)
      : name(aname), type(atype), depth(adepth) {}
  static Col makeVector(const Col &item) { return Col(item.name, "vec<" + item.type + ">", item.depth + 1); }
  static std::vector<Col> makeVectors(const std::vector<Col> &items) {
    std::vector<Col> ret;
    for (auto &c : items)
      ret.push_back(makeVector(c));
    return ret;
  }
  Col addPrefix(const std::string &prefix) const { return Col(prefix + name, type, depth); }
  static std::vector<Col> addPrefix(const std::string &prefix, const std::vector<Col> items) {
    std::vector<Col> ret;
    for (auto &c : items)
      ret.push_back(c.addPrefix(prefix));
    return ret;
  }
};

std::vector<Col> makeColumns(const std::string &name, const arrow::DataType &type) {
  std::vector<Col> ret;
  if (dynamic_cast<const arrow::BooleanType *>(&type) != nullptr) {
    ret.emplace_back(name, type.name());
  } else if (dynamic_cast<const arrow::NumberType *>(&type) != nullptr) {
    ret.emplace_back(name, type.name());
  } else if (dynamic_cast<const arrow::ListType *>(&type) != nullptr) {
    ret.emplace_back("#" + name, "count");
    auto subtype = dynamic_cast<const arrow::ListType &>(type).value_type();
    auto subcols = Col::makeVectors(makeColumns(name, *subtype));
    ret.insert(ret.end(), subcols.begin(), subcols.end());
  } else if (dynamic_cast<const arrow::StructType *>(&type) != nullptr) {
    const arrow::StructType &strtype = dynamic_cast<const arrow::StructType &>(type);
    for (int i = 0, n = strtype.num_fields(); i < n; ++i) {
      auto field = strtype.field(i);
      auto type = field->type();
      auto strcols = makeColumns(name + "." + field->name(), *type);
      ret.insert(ret.end(), strcols.begin(), strcols.end());
    }
  } else {
    std::cout << "Didn't implement " << name << " type " << type.ToString() << std::endl;
  }
  return ret;
}

Status run_ipc(const std::string &type, const char *fname) {
  std::shared_ptr<arrow::io::ReadableFile> file;
  std::cout << "Reading " << fname << std::endl;
  ARROW_ASSIGN_OR_RAISE(file, arrow::io::ReadableFile::Open(fname));
  std::cout << "Opened file " << fname << std::endl;
  std::shared_ptr<arrow::ipc::RecordBatchStreamReader> streamreader;
  std::shared_ptr<arrow::ipc::RecordBatchFileReader> filereader;
  std::shared_ptr<arrow::Schema> schema;
  if (type == "ipcstream") {
    streamreader = *arrow::ipc::RecordBatchStreamReader::Open(file);
    schema = streamreader->schema();
  } else if (type == "ipcfile") {
    filereader = *arrow::ipc::RecordBatchFileReader::Open(file);
    schema = filereader->schema();
  }
  std::cout << "Created reader " << std::endl;
  std::cout << "Found a schema with " << schema->num_fields() << " fields\n";
  for (int i = 0, n = schema->num_fields(); i < n; ++i) {
    auto field = schema->field(i);
    std::cout << "  field[" << i << "]: " << field->name() << ", type " << field->type()->name() << " / "
              << field->type()->ToString() << std::endl;
    auto cols = makeColumns(field->name(), *field->type());
    for (auto c : cols) {
      std::cout << "Col:  " << c.name << ", type " << c.type << ", vector depth " << c.depth << std::endl;
    }
  }
  std::shared_ptr<arrow::RecordBatch> rrb;
  bool first = true;
  if (streamreader) {
    for (auto stat = streamreader->ReadNext(&rrb); stat.ok(); stat = streamreader->ReadNext(&rrb)) {
      std::cout << "Read recordbatch of " << rrb->num_rows() << " rows, " << rrb->num_columns() << " columns"
                << std::endl;
      if (first) {
        arrow::PrettyPrint(*rrb, {}, &std::cout);
        first = false;
      }
    }
  } else if (filereader) {
    std::cout << "Reading " << filereader->num_record_batches() << " batches:" << std::endl;
    for (unsigned int i = 0, n = filereader->num_record_batches(); i < n; ++i) {
      rrb = *filereader->ReadRecordBatch(i);
      std::cout << "Read recordbatch of " << rrb->num_rows() << " rows, " << rrb->num_columns() << " columns"
                << std::endl;
    }
  }

  return Status::OK();
}
int main(int argc, char **argv) {
  if (argc < 3) {
    usage();
    return 1;
  }
  unsigned int iothreads = -1, cputhreads = -1;
  while (1) {
    static struct option long_options[] = {{"help", no_argument, nullptr, 'h'},
                                           {"iothreads", required_argument, nullptr, 'J'},
                                           {"cputhreads", required_argument, nullptr, 'j'},
                                           {nullptr, 0, nullptr, 0}};
    int option_index = 0;
    int optc = getopt_long(argc, argv, "hj:J:b:", long_options, &option_index);
    if (optc == -1)
      break;

    switch (optc) {
      case 'h':
        usage();
        return 0;
      case 'J':
        iothreads = std::atoi(optarg);
        arrow::io::SetIOThreadPoolCapacity(iothreads);
        break;
      case 'j':
        cputhreads = std::atoi(optarg);
        arrow::SetCpuThreadPoolCapacity(cputhreads);
        break;
      default:
        usage();
        return 1;
    }
  }
  int iarg = optind, narg = argc - optind;
  std::string format = argv[iarg];
  std::chrono::time_point<std::chrono::steady_clock> tstart, tend;
  tstart = std::chrono::steady_clock::now();
  if (format == "ipcstream" || format == "ipcfile") {
    Status st = run_ipc(format, argv[iarg + 1]);
    if (!st.ok()) {
      std::cout << st << std::endl;
      return 1;
    }
  }
  tend = std::chrono::steady_clock::now();
  double time = (std::chrono::duration<double>(tend - tstart)).count();
  return 0;
}