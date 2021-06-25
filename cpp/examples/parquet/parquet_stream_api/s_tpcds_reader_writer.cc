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

#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <utility>
#include <fstream>

#include "arrow/io/file.h"
#include "parquet/exception.h"
#include "parquet/stream_reader.h"
#include "parquet/stream_writer.h"
#include <parquet/arrow/reader.h>
#include <arrow/ipc/api.h>
#include "arrow/ipc/options.h"
#include "arrow/ipc/writer.h"
#include "arrow/util/compression.h"
#include "arrow/type_fwd.h"
#include "arrow/table.h"
#include "arrow/ipc/feather.h"

// This file gives an example of how to use the parquet::StreamWriter
// and parquet::StreamReader classes.
// It shows writing/reading of the supported types as well as how a
// user-defined type can be handled.

template <typename T>
using optional = parquet::StreamReader::optional<T>;

// Example of a user-defined type to be written to/read from Parquet
// using C++ input/output operators.
class UserTimestamp {
 public:
  UserTimestamp() = default;

  UserTimestamp(const std::chrono::microseconds v) : ts_{v} {}

  bool operator==(const UserTimestamp& x) const { return ts_ == x.ts_; }

  void dump(std::ostream& os) const {
    const auto t = static_cast<std::time_t>(
        std::chrono::duration_cast<std::chrono::seconds>(ts_).count());
    os << std::put_time(std::gmtime(&t), "%Y%m%d-%H%M%S");
  }

  void dump(parquet::StreamWriter& os) const { os << ts_; }

 private:
  std::chrono::microseconds ts_;
};

std::ostream& operator<<(std::ostream& os, const UserTimestamp& v) {
  v.dump(os);
  return os;
}

parquet::StreamWriter& operator<<(parquet::StreamWriter& os, const UserTimestamp& v) {
  v.dump(os);
  return os;
}

parquet::StreamReader& operator>>(parquet::StreamReader& os, UserTimestamp& v) {
  std::chrono::microseconds ts;

  os >> ts;
  v = UserTimestamp{ts};

  return os;
}

std::vector<std::string> getStrAndSplitIntoTokens(std::string str)
{
  std::vector<std::string>   result;
  std::string                line= str;
  std::stringstream          lineStream(line);
  std::string                cell;

  while(std::getline(lineStream,cell, '|'))
  {
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


std::vector<std::string> getNextLineAndSplitIntoTokens(std::istream& str)
{
  std::vector<std::string>   result;
  std::string                line;
  std::getline(str,line);
//  std::cout << line << std::endl;
  std::stringstream          lineStream(line);
  std::string                cell;

  while(std::getline(lineStream,cell, '|'))
  {
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
          parquet::schema::GroupNode::Make("schema", parquet::Repetition::REQUIRED, fields));
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
  static UserTimestamp GetUserTimestamp(const int i) {
    return UserTimestamp{std::chrono::microseconds{(ts_offset_ + 3 * i) * 1000000 + i}};
  }
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

void WriteParquetFile() {
  std::shared_ptr<arrow::io::FileOutputStream> outfile;
  std::string f_name = "/mnt/dataset1/catalog_sales_sample";

  PARQUET_ASSIGN_OR_THROW(
      outfile,
      arrow::io::FileOutputStream::Open(f_name+".parquet"));

  parquet::WriterProperties::Builder builder;

#if defined ARROW_WITH_BROTLI
  builder.compression(parquet::Compression::BROTLI);
#elif defined ARROW_WITH_ZSTD
  builder.compression(parquet::Compression::ZSTD);
#endif
  auto table = GetCSSchema();

  parquet::StreamWriter os{
      parquet::ParquetFileWriter::Open(outfile, table, builder.build())};


  os.SetMaxRowGroupSize(10000);

  std::ifstream ifile(f_name+".dat");


  int n_col = table->field_count();
  std::cout << "vector size: " << n_col << std::endl;
  int rc = 0;
  std::string line;
  while (std::getline(ifile,line)) {
    auto vec = getStrAndSplitIntoTokens(line);
//    std::cout << "vector size: "+vec.size() << std::endl;
    for (int i=0;i<n_col;i++){
//      std::cout << vec[i] << std::endl;
      if (vec[i]==""){
        os.SkipColumns(1);
        continue;
      }
      auto cur = std::static_pointer_cast<parquet::schema::PrimitiveNode>(table->field(i));
      switch(cur->physical_type()){
        case parquet::Type::INT32:
          os << std::stoi(vec[i]);
          break;
        case parquet::Type::INT64:
          os << static_cast<int64_t>(std::stoll(vec[i], NULL, 10));
          break;
//        case parquet::Type::INT96:


        case parquet::Type::FLOAT:
          os << std::stof(vec[i]);
          break;

        case parquet::Type::DOUBLE:
          os << std::stod(vec[i]);
          break;

        case parquet::Type::BYTE_ARRAY:
          os << vec[i];
          break;

        case parquet::Type::FIXED_LEN_BYTE_ARRAY:
          os << vec[i];
          break;

        default:
          throw parquet::ParquetException("Unexpected type: " + TypeToString(cur->physical_type()));
          break;
      }
    }
    os << parquet::EndRow;
//    if (rc%1000000 == 0) {
//      os << parquet::EndRowGroup;
//      rc=0;
//      std::cout << rc << std::endl;
//    }
//    std::cout << rc << std::endl;
    rc++;
  }
  ifile.close();
  std::cout << "close input file." << std::endl;
  os << parquet::EndRowGroup;
  std::cout << "close input file." << std::endl;
  std::cout << "Parquet Stream Writing complete." << std::endl;
}

void ReadParquetFile() {
  std::shared_ptr<arrow::io::ReadableFile> infile;

  PARQUET_ASSIGN_OR_THROW(
      infile,
      arrow::io::ReadableFile::Open("parquet-stream-api-example.parquet"));

  parquet::StreamReader os{parquet::ParquetFileReader::Open(infile)};


  optional<std::string> opt_string;
  char ch;
  char char_array[4];
  int8_t int8;
  uint16_t uint16;
  int32_t int32;
  optional<uint64_t> opt_uint64;
  double d;
  UserTimestamp ts_user;
  std::chrono::milliseconds ts_ms;
  int i;

  for (i = 0; !os.eof(); ++i) {
    os >> opt_string;
    os >> ch;
    os >> char_array;
    os >> int8;
    os >> uint16;
    os >> int32;
    os >> opt_uint64;
    os >> d;
    os >> ts_user;
    os >> ts_ms;
    os >> parquet::EndRow;

    if (0) {
      // For debugging.
      std::cout << "Row #" << i << std::endl;

      std::cout << "string[";
      if (opt_string) {
        std::cout << *opt_string;
      } else {
        std::cout << "N/A";
      }
      std::cout << "] char[" << ch << "] charArray[" << char_array << "] int8["
                << int(int8) << "] uint16[" << uint16 << "] int32[" << int32;
      std::cout << "] uint64[";
      if (opt_uint64) {
        std::cout << *opt_uint64;
      } else {
        std::cout << "N/A";
      }
      std::cout << "] double[" << d << "] tsUser[" << ts_user << "] tsMs["
                << ts_ms.count() << "]" << std::endl;
    }
    // Check data.
    switch (i % 3) {
      case 0:
        assert(opt_string == TestData::GetOptString(i));
        break;
      case 1:
        assert(*opt_string == TestData::GetStringView(i));
        break;
      case 2:
        assert(*opt_string == TestData::GetCharPtr(i));
        break;
    }
    assert(ch == TestData::GetChar(i));
    switch (i % 2) {
      case 0:
        assert(0 == std::memcmp(char_array, TestData::char4_array, sizeof(char_array)));
        break;
      case 1:
        assert(0 == std::memcmp(char_array, TestData::GetCharPtr(i), sizeof(char_array)));
        break;
    }
    assert(int8 == TestData::GetInt8(i));
    assert(uint16 == TestData::GetUInt16(i));
    assert(int32 == TestData::GetInt32(i));
    assert(opt_uint64 == TestData::GetOptUInt64(i));
    assert(std::abs(d - TestData::GetDouble(i)) < 1e-6);
    assert(ts_user == TestData::GetUserTimestamp(i));
    assert(ts_ms == TestData::GetChronoMilliseconds(i));
  }
  assert(TestData::num_rows == i);

  std::cout << "Parquet Stream Reading complete." << std::endl;
}

// read
arrow::Status read_Parquet2Arrow_file() {

  std::shared_ptr<arrow::io::ReadableFile> infile;
  std::string f_name = "/mnt/dataset1/catalog_sales_sample";
  std::cout << f_name<<".parquet at once" << std::endl;
  PARQUET_ASSIGN_OR_THROW(
          infile,
          arrow::io::ReadableFile::Open(f_name+".parquet",
                                        arrow::default_memory_pool()));

  std::unique_ptr<parquet::arrow::FileReader> reader;
  PARQUET_THROW_NOT_OK(
          parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));
  std::shared_ptr<arrow::Table> table;

  PARQUET_THROW_NOT_OK(reader->ReadTable(&table));

//  std::cout << "name " << table->schema()->ToString()  << std::endl;
  std::shared_ptr<arrow::io::FileOutputStream> outfile;


  PARQUET_ASSIGN_OR_THROW(
          outfile,
          arrow::io::FileOutputStream::Open(f_name+".arrow"));



//  std::cout << "Loaded " << table->num_rows() << " rows in " << table->num_columns()
//            << " columns." << std::endl;
//  arrow::ipc::IpcWriteOptions ipc_options = arrow::ipc::IpcWriteOptions::Defaults();
//  ipc_options.unify_dictionaries = true;
//  ipc_options.allow_64bit = true;
//  int64_t chunksize = 1LL << 16;

//  arrow::Compression::type compression = arrow::Compression::GZIP;
//  int compression_level = std::numeric_limits<int>::min();

//  ARROW_ASSIGN_OR_RAISE(
//          ipc_options.codec,
//          arrow::util::Codec::Create(compression, compression_level));
  arrow::ipc::feather::WriteProperties prop = arrow::ipc::feather::WriteProperties::Defaults();
  prop.version = arrow::ipc::feather::kFeatherV2Version;
  prop.compression = arrow::Compression::ZSTD;
  prop.compression_level = 4 ;

  std::cout<<"apply compression "<< prop.compression <<"with "<< prop.compression_level<<std::endl;

  ARROW_RETURN_NOT_OK(arrow::ipc::feather::WriteTable(*table, outfile.get(),prop));

//  std::shared_ptr<arrow::ipc::RecordBatchWriter> writer;
//  ARROW_ASSIGN_OR_RAISE(writer, MakeFileWriter(outfile, table->schema(), ipc_options));
//  RETURN_NOT_OK(writer->WriteTable(*table, chunksize));
  return outfile->Close();
}


void read_whole_parquet_file() {
  std::cout << "Reading parquet at once" << std::endl;
  std::shared_ptr<arrow::io::ReadableFile> infile;
  std::string f_name = "/mnt/dataset1/catalog_sales_sample";
  PARQUET_ASSIGN_OR_THROW(
          infile,
          arrow::io::ReadableFile::Open(f_name+".parquet",
                                        arrow::default_memory_pool()));

  std::unique_ptr<parquet::arrow::FileReader> reader;
  PARQUET_THROW_NOT_OK(
          parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));
  std::shared_ptr<arrow::Table> table;
  PARQUET_THROW_NOT_OK(reader->ReadTable(&table));
  std::cout << "Loaded " << table->column(33).get()->ToString() << " rows in " << table->num_columns()
            << " columns." << std::endl;
  std::cout << "Loaded " << table->num_rows() << " rows in " << table->num_columns()
            << " columns." << std::endl;
}

int main() {
  std::cout << "Parquet Stream writing started." << std::endl;
  WriteParquetFile();
  std::cout << "Parquet Stream Reading started." << std::endl;
//  ReadParquetFile();
  read_Parquet2Arrow_file();
//  read_whole_parquet_file();
  std::cout << "Parsquet Stream Reading and Writing complete." << std::endl;
  return 0;
}
