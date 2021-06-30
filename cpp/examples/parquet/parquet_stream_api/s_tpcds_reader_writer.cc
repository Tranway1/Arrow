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
#include <chrono>

#include "arrow/io/file.h"
#include "parquet/exception.h"
#include "parquet/stream_reader.h"
#include "parquet/stream_writer.h"
#include <parquet/arrow/reader.h>
#include <arrow/ipc/api.h>
#include <arrow/filesystem/localfs.h>
#include "arrow/ipc/options.h"
#include "arrow/ipc/writer.h"
#include "arrow/util/compression.h"
#include "arrow/type_fwd.h"
#include "arrow/table.h"
#include "arrow/ipc/feather.h"
#include <arrow/api.h>
using arrow::DoubleBuilder;
using arrow::Int32Builder;
const int PRED = 40000;
const int LOC = 700000;
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



std::string Get_Parquet_File(std::string f_name, std::string comp) {
  return f_name+"_"+comp+".parquet";
}


std::string Get_Arrow_File(std::string f_name, std::string comp) {
  return f_name+"_"+comp+".arrow";
}


arrow::Status WriteParquetFile(std::string f_name, std::string comp, int comp_level = std::numeric_limits<int>::min()) {
  std::shared_ptr<arrow::io::FileOutputStream> outfile;
  std::string p_name = Get_Parquet_File(f_name,comp);
  std::cout << p_name << std::endl;
  arrow::Compression::type compression;
  ARROW_ASSIGN_OR_RAISE(compression, arrow::util::Codec::GetCompressionType(comp));
  PARQUET_ASSIGN_OR_THROW(
      outfile,
      arrow::io::FileOutputStream::Open(p_name));

  parquet::WriterProperties::Builder builder;
  builder.compression(compression);
  if (comp_level!=std::numeric_limits<int>::min()){
    builder.compression_level(comp_level);
  }
//  builder.encoding();

#if defined ARROW_WITH_BROTLI
  builder.compression(parquet::Compression::BROTLI);
#elif defined ARROW_WITH_ZSTD
  builder.compression(parquet::Compression::ZSTD);
#endif
  auto table = GetCSSchema();

  parquet::StreamWriter os{
      parquet::ParquetFileWriter::Open(outfile, table, builder.build())};


//  os.SetMaxRowGroupSize(10000);

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
//  os << parquet::EndRowGroup;
  std::cout << "close input file." << std::endl;
  std::cout << "Parquet Stream Writing complete." << std::endl;

  return arrow::Status::OK();
}

void FilterParquetFile(std::string filename) {

  std::unique_ptr<parquet::ParquetFileReader> parquet_reader =
          parquet::ParquetFileReader::OpenFile(filename, false);

  std::shared_ptr<parquet::ColumnReader> column_reader;
  int n_rowgroup = parquet_reader->metadata()->num_row_groups();
  int qualified = 0;
  int16_t definition_level;
  int16_t repetition_level;
  std::cout << "# of row: "<< parquet_reader->metadata()->num_rows()<< std::endl;
  std::cout << "# of row group: "<< n_rowgroup<< std::endl;

  for (int i=0;i<n_rowgroup;i++){
    std::cout << "row group: "<< i<< std::endl;
    column_reader = parquet_reader->RowGroup(i)->Column(1);
    parquet::Int32Reader* int32_reader =
            static_cast<parquet::Int32Reader*>(column_reader.get());

    int64_t  values_read = 0;

    while (int32_reader->HasNext()) {
      int32_t value;
      // Read one value at a time. The number of rows read is returned. values_read
      // contains the number of non-null rows
      int32_reader->ReadBatch(1, &definition_level, &repetition_level, &value, &values_read);
      // There are no NULL values in the rows written
      if (values_read == 0){
        continue;
      }
//      std::cout << value << std::endl;
      if (value>PRED){
        qualified++;
      }

    }
  }
  std::cout << "number of value grater than "<<PRED<<": "<< qualified<< std::endl;
}


void SumParquetFile(std::string filename) {

  std::unique_ptr<parquet::ParquetFileReader> parquet_reader =
          parquet::ParquetFileReader::OpenFile(filename, false);

  std::shared_ptr<parquet::ColumnReader> column_reader;
  int n_rowgroup = parquet_reader->metadata()->num_row_groups();
  int sum = 0;
  int16_t definition_level;
  int16_t repetition_level;
  std::cout << "# of row: "<< parquet_reader->metadata()->num_rows()<< std::endl;
  std::cout << "# of row group: "<< n_rowgroup<< std::endl;

  for (int i=0;i<n_rowgroup;i++){
    std::cout << "row group: "<< i<< std::endl;
    column_reader = parquet_reader->RowGroup(i)->Column(1);
    parquet::Int32Reader* int32_reader =
            static_cast<parquet::Int32Reader*>(column_reader.get());

    int64_t  values_read = 0;

    while (int32_reader->HasNext()) {
      int32_t value;
      // Read one value at a time. The number of rows read is returned. values_read
      // contains the number of non-null rows
      int32_reader->ReadBatch(1, &definition_level, &repetition_level, &value, &values_read);
      // There are no NULL values in the rows written
      if (values_read == 0){
        continue;
      }
//      std::cout << value << std::endl;
      sum+=value;

    }
  }
  std::cout << "Sum parquet: "<< sum<< std::endl;
}

void LookupParquetFile(std::string filename, int loc) {

  std::unique_ptr<parquet::ParquetFileReader> parquet_reader =
          parquet::ParquetFileReader::OpenFile(filename, false);

  std::shared_ptr<parquet::ColumnReader> column_reader;
  int n_rowgroup = parquet_reader->metadata()->num_row_groups();
  int16_t definition_level;
  int16_t repetition_level;

  int32_t value;

  int n_row = parquet_reader->metadata()->num_rows();
  std::cout << "# of row: "<< n_row << std::endl;
  assert(loc<n_row);
  std::cout << "# of row group: "<< n_rowgroup<< std::endl;

  int row_cnt = 0;
  for (int i=0;i<n_rowgroup;i++){
    if (row_cnt + parquet_reader->RowGroup(i)->metadata()->num_rows()<loc+1){
      row_cnt+=parquet_reader->RowGroup(i)->metadata()->num_rows();
      continue;
    }
    std::cout << "row group: "<< i<< std::endl;
    column_reader = parquet_reader->RowGroup(i)->Column(1);
    parquet::Int32Reader* int32_reader =
            static_cast<parquet::Int32Reader*>(column_reader.get());

    int64_t  values_read = 0;


    // Read one value at a time. The number of rows read is returned. values_read
    // contains the number of non-null rows
    int32_reader->Skip(loc-row_cnt);

    int32_reader->ReadBatch(1, &definition_level, &repetition_level, &value, &values_read);
    break;
  }
  std::cout << "value lookup for row "<<loc<<": "<< value << std::endl;


}


// load and read parquet file, parse into arrow and save with arrow feather.
arrow::Status read_Parquet2ArrowDisk(std::string f_name, std::string comp, int comp_level = std::numeric_limits<int>::min()) {

  std::shared_ptr<arrow::io::ReadableFile> infile;
  std::cout << f_name<<".parquet at once" << std::endl;
  PARQUET_ASSIGN_OR_THROW(
          infile,arrow::io::ReadableFile::Open(Get_Parquet_File(f_name,comp),arrow::default_memory_pool()));

  std::unique_ptr<parquet::arrow::FileReader> reader;
  PARQUET_THROW_NOT_OK(
          parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));
  std::shared_ptr<arrow::Table> table;

  PARQUET_THROW_NOT_OK(reader->ReadTable(&table));

//  std::cout << "name " << table->schema()->ToString()  << std::endl;
  std::shared_ptr<arrow::io::FileOutputStream> outfile;


  PARQUET_ASSIGN_OR_THROW(
          outfile,
          arrow::io::FileOutputStream::Open(f_name+"_"+comp+".arrow"));
//          arrow::io::FileOutputStream::Open("/dev/null"));

  arrow::ipc::feather::WriteProperties prop = arrow::ipc::feather::WriteProperties::Defaults();
  arrow::Compression::type compression;
  ARROW_ASSIGN_OR_RAISE(compression, arrow::util::Codec::GetCompressionType(comp));
  prop.version = arrow::ipc::feather::kFeatherV2Version;
  prop.compression = compression;
  if (comp_level!=std::numeric_limits<int>::min()){
    prop.compression_level = comp_level;
  }
//  prop.compression = arrow::Compression::ZSTD;
//  prop.compression_level = 4 ;

  std::cout<<"apply compression "<< prop.compression <<"with "<< prop.compression_level<<std::endl;

  ARROW_RETURN_NOT_OK(arrow::ipc::feather::WriteTable(*table, outfile.get(),prop));

  return outfile->Close();
}

// load and read parquet file, parse into arrow and save with arrow feather in memory.
arrow::Status read_Parquet2ArrowMem(std::string f_name, std::string comp, int comp_level = std::numeric_limits<int>::min()) {

  std::shared_ptr<arrow::io::ReadableFile> infile;
  std::cout << f_name<<".parquet at once" << std::endl;
  PARQUET_ASSIGN_OR_THROW(
          infile,arrow::io::ReadableFile::Open(Get_Parquet_File(f_name,comp),arrow::default_memory_pool()));

  std::unique_ptr<parquet::arrow::FileReader> reader;
  PARQUET_THROW_NOT_OK(
          parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));
  std::shared_ptr<arrow::Table> table;

  PARQUET_THROW_NOT_OK(reader->ReadTable(&table));

//  std::cout << "name " << table->schema()->ToString()  << std::endl;
  std::shared_ptr<arrow::io::FileOutputStream> outfile;


  PARQUET_ASSIGN_OR_THROW(
          outfile,
//          arrow::io::FileOutputStream::Open(f_name+"_"+comp+".arrow"));
          arrow::io::FileOutputStream::Open("/dev/null"));

  arrow::ipc::feather::WriteProperties prop = arrow::ipc::feather::WriteProperties::Defaults();
  arrow::Compression::type compression;
  ARROW_ASSIGN_OR_RAISE(compression, arrow::util::Codec::GetCompressionType(comp));
  prop.version = arrow::ipc::feather::kFeatherV2Version;
  prop.compression = compression;
  if (comp_level!=std::numeric_limits<int>::min()){
    prop.compression_level = comp_level;
  }
//  prop.compression = arrow::Compression::ZSTD;
//  prop.compression_level = 4 ;

  std::cout<<"apply compression "<< prop.compression <<"with "<< prop.compression_level<<std::endl;

  ARROW_RETURN_NOT_OK(arrow::ipc::feather::WriteTable(*table, outfile.get(),prop));

  return outfile->Close();
}

void read_feather_to_table(std::string path,std::shared_ptr<arrow::Table> *feather_table){

  arrow::fs::LocalFileSystem file_system;
  std::shared_ptr <arrow::io::RandomAccessFile> input_file = file_system.OpenInputFile(path).ValueOrDie();
  std::shared_ptr <arrow::ipc::feather::Reader> feather_reader = arrow::ipc::feather::Reader::Open(input_file).ValueOrDie();
  arrow::Status temp_status = feather_reader -> Read(feather_table);
  if(temp_status.ok()){
    std::cout << "Read feather file Successfully." << std::endl;
//    std::cout << ((*feather_table)->schema()) -> ToString() << std::endl; // this line gives segfault
  }
  else{
    std::cout << "Feather file reading process failed." << std::endl;
  }
  return;
}


void sum_feather2table(std::string f_name){
  std::shared_ptr<arrow::Table> table;
  read_feather_to_table(f_name, &table);
  auto n_chunk = table->column(1)->num_chunks();
  int sum = 0;
  std::cout << "# of row: "<< table->num_rows() << std::endl;
  std::cout << "# of chunk: "<< n_chunk<< std::endl;
  for (int i=0;i<n_chunk;i++){
    auto target_col =
            std::static_pointer_cast<arrow::Int32Array>(table->column(1)->chunk(i));

    for (int64_t idx = 0; idx < target_col->length(); idx++){
      int val = target_col->Value(idx);
      sum+=val;
    }

  }
  std::cout << "sum feather: "<< sum<< std::endl;
}


void read_feather2table(std::string f_name){
  std::shared_ptr<arrow::Table> table;
  read_feather_to_table(f_name, &table);
  auto n_chunk = table->column(1)->num_chunks();
  int qualified = 0;
  std::cout << "# of row: "<< table->num_rows() << std::endl;
  std::cout << "# of chunk: "<< n_chunk<< std::endl;
  for (int i=0;i<n_chunk;i++){
    auto target_col =
            std::static_pointer_cast<arrow::Int32Array>(table->column(1)->chunk(i));

    for (int64_t idx = 0; idx < target_col->length(); idx++){

      int val = target_col->Value(idx);
      if (val>PRED){
        qualified++;
      }
    }

  }
  std::cout << "number of value grater than "<<PRED<<": "<< qualified<< std::endl;
}


// lookup query on target column
void lookup_feather2table(std::string f_name, int loc){


  std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
  std::shared_ptr<arrow::Table> table;
  read_feather_to_table(f_name, &table);
  std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
  auto load_arrow = std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count();
  std::cout << "time elapsed in loading arrow: "<< load_arrow << std::endl;

  auto n_chunk = table->column(1)->num_chunks();
  int n_rows =table->num_rows();
  std::cout << "# of row: "<< n_rows << std::endl;
  assert(n_rows>loc);
  std::cout << "# of chunk: "<< n_chunk<< std::endl;
  int row_cnt = 0;
  int val = 0;
  for (int i=0;i<n_chunk;i++){
    if (row_cnt+table->column(1)->chunk(i)->length()<loc+1){
      row_cnt+=table->column(1)->chunk(i)->length();
      continue;
    }
    auto target_col =
            std::static_pointer_cast<arrow::Int32Array>(table->column(1)->chunk(i));
    val = target_col->Value(loc-row_cnt);
    break;
  }
  std::cout << "value lookup for row "<<loc<<": "<< val << std::endl;
}



void read_whole_parquet_file(std::string f_name) {
  std::cout << "Reading parquet at once" << std::endl;
  std::shared_ptr<arrow::io::ReadableFile> infile;
  PARQUET_ASSIGN_OR_THROW(
          infile,
          arrow::io::ReadableFile::Open(f_name,
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

int main(int argc, char** argv) {
  std::cout << "You have entered " << argc
       << " arguments:" << "\n";

  for (int i = 0; i < argc; ++i)
    std::cout << argv[i] << "\n";

  std::string f_name = argv[1];
  std::string comp = argv[2];
  int compression_level = std::numeric_limits<int>::min();
  if (argc>3) {
    compression_level = std::stoi (argv[3]);
  }
  std::cout << "compression " << comp
            << " with level:"<< compression_level << "\n";



  std::cout << "Parquet Stream writing started." << std::endl;
  std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
  auto res = WriteParquetFile(f_name, comp,compression_level);
  std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
  auto t_p_w = std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count();

  std::cout << "Writing Parquet to Arrow feather and save to disk." << std::endl;
  begin = std::chrono::steady_clock::now();
  read_Parquet2ArrowDisk(f_name,comp,compression_level);
  end = std::chrono::steady_clock::now();
  auto t_p2a_d = std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count();

  std::cout << "Writing Parquet to Arrow feather and save to m." << std::endl;
  begin = std::chrono::steady_clock::now();
  read_Parquet2ArrowMem(f_name,comp,compression_level);
  end = std::chrono::steady_clock::now();
  auto t_p2a_m = std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count();

  std::cout << "Filtering Parquet" << std::endl;
  begin = std::chrono::steady_clock::now();
  FilterParquetFile(Get_Parquet_File(f_name,comp));
  end = std::chrono::steady_clock::now();
  auto t_p_r = std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count();

  std::cout << "Filtering arrow" << std::endl;
  begin = std::chrono::steady_clock::now();
  read_feather2table(Get_Arrow_File(f_name,comp));
  end = std::chrono::steady_clock::now();
  auto t_a_r = std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count();

  std::cout << "sum parquet" << std::endl;
  begin = std::chrono::steady_clock::now();
  SumParquetFile(Get_Parquet_File(f_name,comp));
  end = std::chrono::steady_clock::now();
  auto t_p_s = std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count();

  std::cout << "sum arrow" << std::endl;
  begin = std::chrono::steady_clock::now();
  sum_feather2table(Get_Arrow_File(f_name,comp));
  end = std::chrono::steady_clock::now();
  auto t_a_s = std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count();

  std::cout << "Lookup parquet" << std::endl;
  begin = std::chrono::steady_clock::now();
  LookupParquetFile(Get_Parquet_File(f_name,comp),LOC);
  end = std::chrono::steady_clock::now();
  auto t_p_l = std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count();

  std::cout << "Lookup arrow" << std::endl;
  begin = std::chrono::steady_clock::now();
  lookup_feather2table(Get_Arrow_File(f_name,comp),LOC);
  end = std::chrono::steady_clock::now();
  auto t_a_l = std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count();



  std::cout << f_name<<","<<comp<<"," <<t_p_w<< "," << t_p2a_d<<"," << t_p2a_m<<","<<t_p_r<<","<<t_a_r<<","<<t_p_l<<","<<t_a_l <<","<<t_p_s<<","<<t_a_s << std::endl;
//  ReadParquetFile();
//
//  read_feather2table();
//  read_whole_parquet_file();
  std::cout << "Parquet/Arrow Reading and Writing complete." << std::endl;
  return 0;
}
