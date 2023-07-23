// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

#include <arrow/api.h>
#include <arrow/filesystem/localfs.h>
#include <arrow/ipc/api.h>
#include <parquet/arrow/reader.h>
#include <sys/time.h>

#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <utility>

#include "arrow/adapters/orc/adapter.h"
#include "arrow/dataset/file_parquet.h"
#include "arrow/io/file.h"
#include "arrow/ipc/feather.h"
#include "arrow/ipc/options.h"
#include "arrow/ipc/writer.h"
#include "arrow/table.h"
#include "arrow/type_fwd.h"
#include "arrow/util/compression.h"
#include "parquet/arrow/writer.h"
#include "parquet/exception.h"
#include "parquet/properties.h"
#include "parquet/stream_reader.h"
#include "parquet/stream_writer.h"
#include "v_util.h"

using arrow::DoubleBuilder;
using arrow::Int32Builder;
using arrow::adapters::orc::ORCFileReader;
const int PRED = 40000;
const int LOC = 7000001;
int32_t DPRED = 70.0;
// This file gives an example of how to use the parquet::StreamWriter
// and parquet::StreamReader classes.
// It shows writing/reading of the supported types as well as how a
// user-defined type can be handled.

template <typename T>
using optional = parquet::StreamReader::optional<T>;

std::vector<std::string> getStrAndSplitIntoTokens(std::string str, char delim = '|') {
  std::vector<std::string> result;
  std::string line = str;
  std::stringstream lineStream(line);
  std::string cell;

  while (std::getline(lineStream, cell, delim)) {
    result.push_back(cell);
  }
  // This checks for a trailing comma wFiltering arrowith no data after it.
  //  if (!lineStream && cell.empty())
  //  {
  //    // If there was a trailing comma then add an empty element.
  //    result.push_back("");
  //  }
  return result;
}

// trim from start (in place)
static inline void ltrim(std::string& s) {
  s.erase(s.begin(), std::find_if(s.begin(), s.end(),
                                  [](unsigned char ch) { return !std::isspace(ch); }));
}

// trim from end (in place)
static inline void rtrim(std::string& s) {
  s.erase(std::find_if(s.rbegin(), s.rend(),
                       [](unsigned char ch) { return !std::isspace(ch); })
              .base(),
          s.end());
}

// trim from both ends (in place)
static inline void trim(std::string& s) {
  ltrim(s);
  rtrim(s);
}

std::vector<std::string> getNextLineAndSplitIntoTokens(std::istream& str) {
  std::vector<std::string> result;
  std::string line;
  std::getline(str, line);
  //  std::cout << line << std::endl;
  std::stringstream lineStream(line);
  std::string cell;

  while (std::getline(lineStream, cell, '|')) {
    result.push_back(cell);
  }
  // This checks for a trailing comma with no data after it.
  //  if (!lineStream && cell.empty())
  //  {
  //    // If there was a trailing comma then add an empty element.
  //    result.push_back("");
  //  }
  return result;
}

std::shared_ptr<parquet::schema::GroupNode> GetCSSchema() {
  parquet::schema::NodeVector fields;
  fields.push_back(parquet::schema::PrimitiveNode::Make(
      "cs_sold_date_sk", parquet::Repetition::OPTIONAL, parquet::Type::INT64,
      parquet::ConvertedType::INT_64));
  fields.push_back(parquet::schema::PrimitiveNode::Make(
      "cs_sold_time_sk", parquet::Repetition::OPTIONAL, parquet::Type::INT32,
      parquet::ConvertedType::INT_32));
  fields.push_back(parquet::schema::PrimitiveNode::Make(
      "cs_ship_date_sk", parquet::Repetition::OPTIONAL, parquet::Type::INT32,
      parquet::ConvertedType::INT_32));
  fields.push_back(parquet::schema::PrimitiveNode::Make(
      "cs_bill_customer_sk", parquet::Repetition::OPTIONAL, parquet::Type::INT32,
      parquet::ConvertedType::INT_32));
  fields.push_back(parquet::schema::PrimitiveNode::Make(
      "cs_bill_cdemo_sk", parquet::Repetition::OPTIONAL, parquet::Type::INT32,
      parquet::ConvertedType::INT_32));
  fields.push_back(parquet::schema::PrimitiveNode::Make(
      "cs_bill_hdemo_sk", parquet::Repetition::OPTIONAL, parquet::Type::INT32,
      parquet::ConvertedType::INT_32));
  fields.push_back(parquet::schema::PrimitiveNode::Make(
      "cs_bill_addr_sk", parquet::Repetition::OPTIONAL, parquet::Type::INT32,
      parquet::ConvertedType::INT_32));
  fields.push_back(parquet::schema::PrimitiveNode::Make(
      "cs_ship_customer_sk", parquet::Repetition::OPTIONAL, parquet::Type::INT32,
      parquet::ConvertedType::INT_32));
  fields.push_back(parquet::schema::PrimitiveNode::Make(
      "cs_ship_cdemo_sk", parquet::Repetition::OPTIONAL, parquet::Type::INT32,
      parquet::ConvertedType::INT_32));
  fields.push_back(parquet::schema::PrimitiveNode::Make(
      "cs_ship_hdemo_sk", parquet::Repetition::OPTIONAL, parquet::Type::INT32,
      parquet::ConvertedType::INT_32));
  fields.push_back(parquet::schema::PrimitiveNode::Make(
      "cs_ship_addr_sk", parquet::Repetition::OPTIONAL, parquet::Type::INT32,
      parquet::ConvertedType::INT_32));
  fields.push_back(parquet::schema::PrimitiveNode::Make(
      "cs_call_center_sk", parquet::Repetition::OPTIONAL, parquet::Type::INT32,
      parquet::ConvertedType::INT_32));
  fields.push_back(parquet::schema::PrimitiveNode::Make(
      "cs_catalog_page_sk", parquet::Repetition::OPTIONAL, parquet::Type::INT32,
      parquet::ConvertedType::INT_32));
  fields.push_back(parquet::schema::PrimitiveNode::Make(
      "cs_ship_mode_sk", parquet::Repetition::OPTIONAL, parquet::Type::INT32,
      parquet::ConvertedType::INT_32));
  fields.push_back(parquet::schema::PrimitiveNode::Make(
      "cs_warehouse_sk", parquet::Repetition::OPTIONAL, parquet::Type::INT32,
      parquet::ConvertedType::INT_32));
  fields.push_back(parquet::schema::PrimitiveNode::Make(
      "cs_item_sk", parquet::Repetition::OPTIONAL, parquet::Type::INT32,
      parquet::ConvertedType::INT_32));
  fields.push_back(parquet::schema::PrimitiveNode::Make(
      "cs_promo_sk", parquet::Repetition::OPTIONAL, parquet::Type::INT32,
      parquet::ConvertedType::INT_32));
  fields.push_back(parquet::schema::PrimitiveNode::Make(
      "cs_order_number", parquet::Repetition::OPTIONAL, parquet::Type::INT64,
      parquet::ConvertedType::INT_64));
  fields.push_back(parquet::schema::PrimitiveNode::Make(
      "cs_quantity", parquet::Repetition::OPTIONAL, parquet::Type::INT32,
      parquet::ConvertedType::INT_32));

  fields.push_back(parquet::schema::PrimitiveNode::Make(
      "cs_wholesale_cost", parquet::Repetition::OPTIONAL, parquet::Type::DOUBLE,
      parquet::ConvertedType::NONE));
  fields.push_back(parquet::schema::PrimitiveNode::Make(
      "cs_list_price", parquet::Repetition::OPTIONAL, parquet::Type::DOUBLE,
      parquet::ConvertedType::NONE));
  fields.push_back(parquet::schema::PrimitiveNode::Make(
      "cs_sales_price", parquet::Repetition::OPTIONAL, parquet::Type::DOUBLE,
      parquet::ConvertedType::NONE));
  fields.push_back(parquet::schema::PrimitiveNode::Make(
      "cs_ext_discount_amt", parquet::Repetition::OPTIONAL, parquet::Type::DOUBLE,
      parquet::ConvertedType::NONE));
  fields.push_back(parquet::schema::PrimitiveNode::Make(
      "cs_ext_sales_price", parquet::Repetition::OPTIONAL, parquet::Type::DOUBLE,
      parquet::ConvertedType::NONE));
  fields.push_back(parquet::schema::PrimitiveNode::Make(
      "cs_ext_wholesale_cost", parquet::Repetition::OPTIONAL, parquet::Type::DOUBLE,
      parquet::ConvertedType::NONE));
  fields.push_back(parquet::schema::PrimitiveNode::Make(
      "cs_ext_list_price", parquet::Repetition::OPTIONAL, parquet::Type::DOUBLE,
      parquet::ConvertedType::NONE));
  fields.push_back(parquet::schema::PrimitiveNode::Make(
      "cs_ext_tax", parquet::Repetition::OPTIONAL, parquet::Type::DOUBLE,
      parquet::ConvertedType::NONE));
  fields.push_back(parquet::schema::PrimitiveNode::Make(
      "cs_coupon_amt", parquet::Repetition::OPTIONAL, parquet::Type::DOUBLE,
      parquet::ConvertedType::NONE));
  fields.push_back(parquet::schema::PrimitiveNode::Make(
      "cs_ext_ship_cost", parquet::Repetition::OPTIONAL, parquet::Type::DOUBLE,
      parquet::ConvertedType::NONE));
  fields.push_back(parquet::schema::PrimitiveNode::Make(
      "cs_net_paid", parquet::Repetition::OPTIONAL, parquet::Type::DOUBLE,
      parquet::ConvertedType::NONE));
  fields.push_back(parquet::schema::PrimitiveNode::Make(
      "cs_net_paid_inc_tax", parquet::Repetition::OPTIONAL, parquet::Type::DOUBLE,
      parquet::ConvertedType::NONE));
  fields.push_back(parquet::schema::PrimitiveNode::Make(
      "cs_net_paid_inc_ship", parquet::Repetition::OPTIONAL, parquet::Type::DOUBLE,
      parquet::ConvertedType::NONE));
  fields.push_back(parquet::schema::PrimitiveNode::Make(
      "cs_net_paid_inc_ship_tax", parquet::Repetition::OPTIONAL, parquet::Type::DOUBLE,
      parquet::ConvertedType::NONE));
  fields.push_back(parquet::schema::PrimitiveNode::Make(
      "cs_net_profit", parquet::Repetition::OPTIONAL, parquet::Type::DOUBLE,
      parquet::ConvertedType::NONE));

  return std::static_pointer_cast<parquet::schema::GroupNode>(
      parquet::schema::GroupNode::Make("catalog_sales", parquet::Repetition::REQUIRED,
                                       fields));
}

std::shared_ptr<parquet::schema::GroupNode> GetSchema() {
  parquet::schema::NodeVector fields;

  fields.push_back(parquet::schema::PrimitiveNode::Make(
      "string_field", parquet::Repetition::OPTIONAL, parquet::Type::BYTE_ARRAY,
      parquet::ConvertedType::UTF8));

  fields.push_back(parquet::schema::PrimitiveNode::Make(
      "char_field", parquet::Repetition::REQUIRED, parquet::Type::FIXED_LEN_BYTE_ARRAY,
      parquet::ConvertedType::NONE, 1));

  fields.push_back(parquet::schema::PrimitiveNode::Make(
      "char[4]_field", parquet::Repetition::REQUIRED, parquet::Type::FIXED_LEN_BYTE_ARRAY,
      parquet::ConvertedType::NONE, 4));

  fields.push_back(parquet::schema::PrimitiveNode::Make(
      "int8_field", parquet::Repetition::REQUIRED, parquet::Type::INT32,
      parquet::ConvertedType::INT_8));

  fields.push_back(parquet::schema::PrimitiveNode::Make(
      "uint16_field", parquet::Repetition::REQUIRED, parquet::Type::INT32,
      parquet::ConvertedType::UINT_16));

  fields.push_back(parquet::schema::PrimitiveNode::Make(
      "int32_field", parquet::Repetition::REQUIRED, parquet::Type::INT32,
      parquet::ConvertedType::INT_32));

  fields.push_back(parquet::schema::PrimitiveNode::Make(
      "uint64_field", parquet::Repetition::OPTIONAL, parquet::Type::INT64,
      parquet::ConvertedType::UINT_64));

  fields.push_back(parquet::schema::PrimitiveNode::Make(
      "double_field", parquet::Repetition::REQUIRED, parquet::Type::DOUBLE,
      parquet::ConvertedType::NONE));

  // User defined timestamp type.
  fields.push_back(parquet::schema::PrimitiveNode::Make(
      "timestamp_field", parquet::Repetition::REQUIRED, parquet::Type::INT64,
      parquet::ConvertedType::TIMESTAMP_MICROS));

  fields.push_back(parquet::schema::PrimitiveNode::Make(
      "chrono_milliseconds_field", parquet::Repetition::REQUIRED, parquet::Type::INT64,
      parquet::ConvertedType::TIMESTAMP_MILLIS));

  return std::static_pointer_cast<parquet::schema::GroupNode>(
      parquet::schema::GroupNode::Make("schema", parquet::Repetition::REQUIRED, fields));
}

struct TestData {
  static const int num_rows = 2000;

  static void init() { std::time(&ts_offset_); }

  static optional<std::string> GetOptString(const int i) {
    if (i % 2 == 0) return {};
    return "Str #" + std::to_string(i);
  }

  static arrow::util::string_view GetStringView(const int i) {
    string_ = "StringView #" + std::to_string(i);
    return arrow::util::string_view(string_);
  }

  static const char* GetCharPtr(const int i) {
    string_ = "CharPtr #" + std::to_string(i);
    return string_.c_str();
  }

  static char GetChar(const int i) { return i & 1 ? 'M' : 'F'; }

  static int8_t GetInt8(const int i) { return static_cast<int8_t>((i % 256) - 128); }

  static uint16_t GetUInt16(const int i) { return static_cast<uint16_t>(i); }

  static int32_t GetInt32(const int i) { return 3 * i - 17; }

  static optional<uint64_t> GetOptUInt64(const int i) {
    if (i % 11 == 0) return {};
    return (1ull << 40) + i * i + 101;
  }

  static double GetDouble(const int i) { return 6.62607004e-34 * 3e8 * i; }

  static std::chrono::milliseconds GetChronoMilliseconds(const int i) {
    return std::chrono::milliseconds{(ts_offset_ + 3 * i) * 1000ull + i};
  }

  static char char4_array[4];

 private:
  static std::time_t ts_offset_;
  static std::string string_;
};

char TestData::char4_array[] = "XYZ";
std::time_t TestData::ts_offset_;
std::string TestData::string_;

class type;

std::string GetFileName(std::string my_str) {
  std::vector<std::string> result;
  std::stringstream s_stream(my_str);  // create string stream from the string
  while (s_stream.good()) {
    std::string substr;
    getline(s_stream, substr, '/');
    result.push_back(substr);
  }
  return result.at(result.size() - 1);
}

int GetStringSchemaLength(std::string str) {
  size_t i = 0;
  for (; i < str.length(); i++) {
    if (isdigit(str[i])) break;
  }
  str = str.substr(i, str.length() - i);
  int id = atoi(str.c_str());
  return id;
}

std::shared_ptr<parquet::schema::GroupNode> GetSchemaFromFile(std::string f_name,
                                                              char delim) {
  parquet::schema::NodeVector fields;

  std::ifstream ifile("/home/chunwei/arrow/cpp/examples/parquet/tpcds/" + f_name +
                      ".schema");
  std::cout << "schema file path: ~/arrow/cpp/examples/parquet/tpcds/" + f_name +
                   ".schema"
            << std::endl;

  std::string line;
  while (std::getline(ifile, line)) {
    trim(line);
    auto vec = getStrAndSplitIntoTokens(line, delim);

    if (vec[vec.size() - 1].find("decimal") != std::string::npos) {
      fields.push_back(
          //                    parquet::schema::PrimitiveNode::Make(vec[0],
          //                    parquet::Repetition::OPTIONAL,
          //                                                         parquet::DecimalLogicalType::Make(7,
          //                                                         2),
          //                                                         parquet::Type::INT32)
          parquet::schema::PrimitiveNode::Make(vec[0], parquet::Repetition::OPTIONAL,
                                               parquet::Type::DOUBLE,
                                               parquet::ConvertedType::NONE));
    } else if (vec[vec.size() - 1].find("integer") != std::string::npos) {
      fields.push_back(parquet::schema::PrimitiveNode::Make(
          vec[0], parquet::Repetition::OPTIONAL, parquet::Type::INT32,
          parquet::ConvertedType::INT_32));
    } else if (vec[vec.size() - 1].find("bigint") != std::string::npos) {
      fields.push_back(parquet::schema::PrimitiveNode::Make(
          vec[0], parquet::Repetition::OPTIONAL, parquet::Type::INT64,
          parquet::ConvertedType::INT_64));
    } else if (vec[vec.size() - 1].find("varchar") != std::string::npos) {
      int len = GetStringSchemaLength(vec[vec.size() - 1]);
      std::cout << "string length: " << len << std::endl;
      fields.push_back(parquet::schema::PrimitiveNode::Make(
          vec[0], parquet::Repetition::OPTIONAL, parquet::Type::BYTE_ARRAY,
          parquet::ConvertedType::UTF8));
    }
  }

  return std::static_pointer_cast<parquet::schema::GroupNode>(
      parquet::schema::GroupNode::Make(f_name, parquet::Repetition::REQUIRED, fields));
}

// original version: load and read parquet file, parse into arrow and save with arrow
// feather.
arrow::Status read_Parquet2ArrowDisk(std::string f_name, std::string comp,
                                     int comp_level = std::numeric_limits<int>::min()) {
  std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();

  std::shared_ptr<arrow::io::ReadableFile> infile;
  std::cout << Get_Parquet_File(f_name, comp) << std::endl;

  PARQUET_ASSIGN_OR_THROW(
      infile, arrow::io::ReadableFile::Open(Get_Parquet_File(f_name, comp),
                                            arrow::default_memory_pool()));

  std::unique_ptr<parquet::arrow::FileReader> reader;
  PARQUET_THROW_NOT_OK(
      parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));
  std::shared_ptr<arrow::Table> table;

//  std::vector<int> vect{3};
  PARQUET_THROW_NOT_OK(reader->ReadTable(&table));
//  PARQUET_THROW_NOT_OK(reader->ReadTable(vect, &table));

  std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
  auto load_arrow =
      std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count();
  std::cout << "time elapsed in loading parquet to arrow: " << load_arrow << std::endl;


  std::cout << "# of columns: " << table->num_columns() << std::endl;

  return table;

}

// original version: load and read orc file, parse into arrow and save with arrow feather.
std::shared_ptr<arrow::Table> read_ORC2ArrowDisk(std::string f_name, std::string comp,
                                 int comp_level = std::numeric_limits<int>::min()) {
  std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
  arrow::fs::LocalFileSystem file_system;
  std::shared_ptr<arrow::io::RandomAccessFile> infile =
      file_system.OpenInputFile(Get_ORC_File(f_name, comp)).ValueOrDie();

  std::cout << Get_ORC_File(f_name, comp) << std::endl;
  std::unique_ptr<ORCFileReader> reader;
  ORCFileReader::Open(infile, arrow::default_memory_pool(), &reader);

  std::shared_ptr<arrow::Table> table;

//  std::vector<int> vect{3};
  PARQUET_THROW_NOT_OK(reader->Read(&table));
//  PARQUET_THROW_NOT_OK(reader->Read(vect, &table));

  std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
  auto load_arrow =
      std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count();
  std::cout << "time elapsed in loading orc to arrow: " << load_arrow << std::endl;

  std::cout << "# of columns: " << table->num_columns() << std::endl;

  return table;

}

std::shared_ptr<arrow::Table> read_feather2table(std::string f_name) {
  std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
  std::shared_ptr<arrow::Table> table;
//  std::vector<int> vect{3};
  read_feather_to_table(f_name, &table);
//      read_feather_column_to_table(f_name, &table, vect);
  std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
  auto load_arrow =
      std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count();
  std::cout << "time elapsed in loading feather to arrow: " << load_arrow << std::endl;

  std::cout << "# of columns: " << table->num_columns() << std::endl;
  return table;

}

void write_parquet_file(const arrow::Table& table, std::string fname) {
  std::shared_ptr<arrow::io::FileOutputStream> outfile;
  PARQUET_ASSIGN_OR_THROW(
      outfile,
      arrow::io::FileOutputStream::Open(fname));
//      arrow::io::FileOutputStream::Open("/dev/null"));
  // The last argument to the function call is the size of the RowGroup in
  // the parquet file. Normally you would choose this to be rather large but
  // for the example, we use a small value to have multiple RowGroups.
  std::shared_ptr<parquet::WriterProperties> properties =
//      parquet::WriterProperties::Builder().compression(parquet::Compression::UNCOMPRESSED)->build();
      parquet::WriterProperties::Builder().compression(parquet::Compression::ZSTD)->compression_level(1)->build();

  PARQUET_THROW_NOT_OK(
      parquet::arrow::WriteTable(table, arrow::default_memory_pool(), outfile, 128 * 1024 * 1024, properties));
  outfile->Close();
}

void write_orc_file(const arrow::Table& table, std::string fname) {
  std::shared_ptr<arrow::io::FileOutputStream> outfile;
  PARQUET_ASSIGN_OR_THROW(
      outfile,
      arrow::io::FileOutputStream::Open(fname));
//      arrow::io::FileOutputStream::Open("/dev/null"));

  auto orcwriter = arrow::adapters::orc::ORCFileWriter::Open(outfile.get());

  PARQUET_THROW_NOT_OK(
      orcwriter->get()->Write(table));
//  outfile->Close();
}


arrow::Status write_feather_file(const arrow::Table& table, std::string fname) {
  std::shared_ptr<arrow::io::FileOutputStream> outfile;
//  std::string comp= "uncompressed";
//  auto comp_level = std::numeric_limits<int>::min();
  std::string comp= "zstd";
  auto comp_level = 1;


  PARQUET_ASSIGN_OR_THROW(
      outfile,
    arrow::io::FileOutputStream::Open(fname));
//      arrow::io::FileOutputStream::Open("/dev/null"));

  std::string arrow_comp = comp;
  if (comp.find("lz4") != std::string::npos) {
    std::cout << "apply lz4_frame for arrow." << std::endl;
    arrow_comp = "lz4";
  }

  arrow::ipc::feather::WriteProperties prop = arrow::ipc::feather::WriteProperties::Defaults();
  arrow::Compression::type compression;
  ARROW_ASSIGN_OR_RAISE(compression, arrow::util::Codec::GetCompressionType(arrow_comp));
  prop.version = arrow::ipc::feather::kFeatherV2Version;
  prop.compression = compression;
  if (comp_level != std::numeric_limits<int>::min()) {
    prop.compression_level = comp_level;
  }
  //  prop.compression = arrow::Compression::ZSTD;
  //  prop.compression_level = 4 ;

  std::cout << "apply compression " << prop.compression << "with " << prop.compression_level << std::endl;

  ARROW_RETURN_NOT_OK(arrow::ipc::feather::WriteTable(table, outfile.get(), prop));

  return outfile->Close();
}


int main(int argc, char** argv) {
  std::cout << "You have entered " << argc << " arguments:"
            << "\n";

  for (int i = 0; i < argc; ++i) std::cout << argv[i] << "\n";

  std::string f_name = argv[1];
  std::string comp = argv[2];
  int compression_level = std::numeric_limits<int>::min();
  std::string file_name = GetFileName(f_name);
  std::cout << "file name is " << file_name << "\n";

  if (argc > 3) {
    compression_level = std::stoi(argv[3]);
  }
  std::cout << "compression " << comp << " with level:" << compression_level << "\n";

  sync();
  std::ofstream ofs("/proc/sys/vm/drop_caches");
  ofs << "3" << std::endl;

  std::cout << "----Writing Parquet to Arrow table." << std::endl;
  auto begin = std::chrono::steady_clock::now();
  auto ptable = read_Parquet2ArrowDisk(f_name, comp, compression_level);
  auto end = std::chrono::steady_clock::now();
  auto t_p2a_d =
      std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count();

  std::cout << "Writing table to Parquet on disk." << std::endl;
  auto rbegin = std::chrono::steady_clock::now();
  write_parquet_file(*ptable, f_name+"table.PARQUET");
  auto rend = std::chrono::steady_clock::now();
  auto t2parquet =
      std::chrono::duration_cast<std::chrono::milliseconds>(rend - rbegin).count();

  sync();
  ofs << "3" << std::endl;


  std::cout << "----Writing ORC to table" << std::endl;
  begin = std::chrono::steady_clock::now();
  auto otable = read_ORC2ArrowDisk(f_name, comp, compression_level);
  end = std::chrono::steady_clock::now();
  auto t_o2a_d =
      std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count();

  std::cout << "Writing Arrow table to ORC on disk." << std::endl;
  rbegin = std::chrono::steady_clock::now();
  write_orc_file(*otable, f_name+"table.ORC");
  rend = std::chrono::steady_clock::now();
  auto t2orc =
      std::chrono::duration_cast<std::chrono::milliseconds>(rend - rbegin).count();


  sync();
  ofs << "3" << std::endl;



  std::cout << "----read feather into table" << std::endl;
  std::cout << Get_Arrow_File(f_name, comp)<< std::endl;
  begin = std::chrono::steady_clock::now();
  auto ftable = read_feather2table(Get_Arrow_File(f_name, comp));
  end = std::chrono::steady_clock::now();
  auto t_a_s = std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count();

  std::cout << "Writing table to feather on disk." << std::endl;
  rbegin = std::chrono::steady_clock::now();
  write_feather_file(*ftable, f_name+"table.FEATHER");
  rend = std::chrono::steady_clock::now();
  auto t2feather =
      std::chrono::duration_cast<std::chrono::milliseconds>(rend - rbegin).count();



  std::cout << f_name << "," << comp << "," << t_p2a_d << "," << t_o2a_d << "," <<  t_a_s << std::endl;
  std::cout << f_name << "," << t2parquet << "," << t2orc << "," << t2feather << std::endl;

  std::cout << "transcoding complete." << std::endl;
  return 0;
}
