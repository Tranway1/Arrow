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
#include <utility>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <fstream>

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
#include <sys/time.h>
#include "arrow/dataset/file_parquet.h"
#include "parquet/properties.h"
#include "v_util.h"
#include <sys/stat.h>



std::string GetFileName(std::string my_str) {
    std::vector<std::string> result;
    std::stringstream s_stream(my_str); //create string stream from the string
    while (s_stream.good()) {
        std::string substr;
        getline(s_stream, substr, '/');
        result.push_back(substr);
    }
    return result.at(result.size() - 1);
}


int GetStringSchemaLength(std::string str) {
    size_t i = 0;
    for (; i < str.length(); i++) { if (isdigit(str[i])) break; }
    str = str.substr(i, str.length() - i);
    int id = atoi(str.c_str());
    return id;
}


void ReadParquetMeta(std::string filename) {

    std::unique_ptr<parquet::ParquetFileReader> parquet_reader =
            parquet::ParquetFileReader::OpenFile(filename, false);
    auto schema = parquet_reader->metadata()->schema();
    int groups = parquet_reader->metadata()->num_row_groups();
    int cols = parquet_reader->metadata()->num_columns();
    parquet::Compression::type comp;
    long comps = 0;
    long uncomps = 0;
    long i_size = 0;
    long d_size = 0;
    long s_size = 0;

    for (int i = 0; i < groups; i++) {
        auto cur_group = parquet_reader->metadata()->RowGroup(i);
        long cs = 0;
        long us = 0;
        for (int j = 0; j < cols; j++) {
            auto c_chunk = cur_group->ColumnChunk(j);
            int compsize = c_chunk->total_compressed_size();
            int uncompsize = c_chunk->total_uncompressed_size();
            comp = c_chunk->compression();
            auto enc = c_chunk->encodings();
            cs += compsize;
            us += uncompsize;
            std::cout << "\t current column chunk " << j << " in row group " << i
                      << " with comp " << comp << " and encoding " << EncodingToString(enc.at(2)) << " on type "
                      << TypeToString(schema->Column(j)->physical_type()) << ", comp size:" << compsize << " comp size:"
                      << uncompsize << std::endl;
            if (schema->Column(j)->physical_type() == 1 || schema->Column(j)->physical_type() == 2 ||
                schema->Column(j)->physical_type() == 3) {
                i_size += compsize;
            } else if (schema->Column(j)->physical_type() == 4 || schema->Column(j)->physical_type() == 5) {
                d_size += compsize;
            } else {
                s_size += compsize;
            }
        }
        std::cout << "current row group " << i << " comp size:" << cur_group->total_compressed_size() << "est: " << cs
                  << " uncomp size:" << cur_group->total_byte_size() << " est: " << us << std::endl;
        comps += cs;
        uncomps += us;
    }

    std::cout << "file size " << parquet_reader->metadata()->size() << " est: " << comps << std::endl;
    std::cout << "****" << comp << "," << filename << "," << i_size << "," << d_size << "," << s_size << ","
              << i_size + d_size + s_size << "," << filesize(filename) << "\n";

}


// original version: load and read parquet file, parse into arrow and save with arrow feather.
arrow::Status read_Parquet2ArrowDisk(std::string f_name, std::string comp, int comp_level = std::numeric_limits<int>::min()) {

    std::shared_ptr<arrow::io::ReadableFile> infile;
    std::cout << f_name << ".parquet at once" << std::endl;

    PARQUET_ASSIGN_OR_THROW(
            infile, arrow::io::ReadableFile::Open(f_name, arrow::default_memory_pool()));

    std::unique_ptr<parquet::arrow::FileReader> reader;
    PARQUET_THROW_NOT_OK(
            parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));
    std::shared_ptr<arrow::Table> table;

    PARQUET_THROW_NOT_OK(reader->ReadTable(&table));

    std::cout << "==file name: " << f_name << "," << table->schema()->ToString()  << std::endl;
    std::shared_ptr<arrow::io::FileOutputStream> outfile;


    PARQUET_ASSIGN_OR_THROW(
            outfile,
            arrow::io::FileOutputStream::Open(Get_Arrow_File(f_name, comp,comp_level)));
//          arrow::io::FileOutputStream::Open("/dev/null"));
    std::cout << "output file " << Get_Arrow_File(f_name, comp,comp_level)  << std::endl;
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

//    std::cout << "apply compression " << prop.compression << "with " << prop.compression_level << std::endl;

    ARROW_RETURN_NOT_OK(arrow::ipc::feather::WriteTable(*table, outfile.get(), prop));

    return outfile->Close();
}


// load and read parquet file, parse into arrow with dictionary and save with arrow feather.
arrow::Status
Parquet2ArrowDict(std::string f_name, std::string comp, int comp_level = std::numeric_limits<int>::min()) {

    std::shared_ptr<arrow::io::ReadableFile> infile;
    std::cout << f_name << ".parquet at once" << std::endl;


    PARQUET_ASSIGN_OR_THROW(
            infile, arrow::io::ReadableFile::Open(f_name, arrow::default_memory_pool()));

    std::unique_ptr<parquet::ParquetFileReader> p_reader;
    try {
        p_reader = parquet::ParquetFileReader::Open(infile);
    } catch (const ::parquet::ParquetException &e) {
        return arrow::Status::IOError("Could not open parquet input source '", Get_Parquet_File(f_name, comp,comp_level),
                                      "': ", e.what());
    }

    std::shared_ptr<parquet::FileMetaData> metadata = p_reader->metadata();
    int n_col = metadata->num_columns();

    parquet::ArrowReaderProperties arg_properties = parquet::default_arrow_reader_properties();
    std::cout << "setting the arrow reader property with col num: " << n_col << std::endl;
    for (int i = 0; i < n_col; i++) {
        arg_properties.set_read_dictionary(i, true);
    }

    std::unique_ptr<parquet::arrow::FileReader> reader;
    // set arrow properties to enable dictionary encoding
    PARQUET_THROW_NOT_OK(
            parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader, arg_properties));
    std::shared_ptr<arrow::Table> table;

    PARQUET_THROW_NOT_OK(reader->ReadTable(&table));

//  std::cout << "name " << table->schema()->ToString()  << std::endl;
    std::shared_ptr<arrow::io::FileOutputStream> outfile;


    PARQUET_ASSIGN_OR_THROW(
            outfile,
            arrow::io::FileOutputStream::Open(f_name + "_" + comp + "_dict.arrow"));
//          arrow::io::FileOutputStream::Open("/dev/null"));

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

    ARROW_RETURN_NOT_OK(arrow::ipc::feather::WriteTable(*table, outfile.get(), prop));

    return outfile->Close();
}

std::vector<std::string> read_file_list(const std::string &file_name) {
  std::ifstream file(file_name);
  std::string line;
  std::vector<std::string> file_list;

  if (file.is_open()) {
    while (std::getline(file, line)) {
      file_list.push_back(line);
    }
    file.close();
  } else {
    std::cerr << "Unable to open file: " << file_name << std::endl;
  }

  return file_list;
}

bool file_exists(const std::string &file_name) {
  struct stat buffer;
  return (stat(file_name.c_str(), &buffer) == 0);
}

int main(int argc, char **argv) {
  std::cout << "You have entered " << argc << " arguments:" << "\n";

  for (int i = 0; i < argc; ++i)
    std::cout << argv[i] << "\n";

  std::string list_file_name = argv[1];
  std::string comp = argv[2];
  int compression_level = std::numeric_limits<int>::min();

  if (argc > 3) {
    compression_level = std::stoi(argv[3]);
  }

  std::vector<std::string> file_list = read_file_list(list_file_name);

  int i = 0;
  int start = 0;
  for (const auto &f_n : file_list) {
    std::string file_name = "/job/columns/"+f_n+".DICT";
    i++;
    if (i<start){
      continue;
    }
    if (!file_exists(file_name)) {
      std::cerr << "File does not exist: " << file_name << std::endl;
      continue;
    }
  //    std::string file_name = GetFileName(f_name);
    try{
      std::cout << "Processing file " << file_name << "\n";
      std::cout << "compression " << comp << " with level:" << compression_level << "\n";

      arrow::Status status;

      status = read_Parquet2ArrowDisk(file_name, comp, compression_level);
      if (!status.ok()) {
        std::cerr << "Error in read_Parquet2ArrowDisk for " << file_name << ": " << status.ToString() << std::endl;
        continue;
      }

      status = Parquet2ArrowDict(file_name, comp, compression_level);
      if (!status.ok()) {
        std::cerr << "Error in Parquet2ArrowDict for " << file_name << ": " << status.ToString() << std::endl;
        continue;
      }

      std::cout << "Parquet/Arrow Reading and Writing for " << file_name << " complete." << std::endl;
    }
    catch (const std::exception &e) {
      std::cerr << "Exception occurred for file " << file_name << ": " << e.what() << std::endl;
      continue;
    }
  }

  return 0;
}

//int main(int argc, char **argv) {
//  std::cout << "You have entered " << argc
//            << " arguments:" << "\n";
//
//  for (int i = 0; i < argc; ++i)
//    std::cout << argv[i] << "\n";
//
//  std::string f_name = argv[1];
//  std::string comp = argv[2];
//  int compression_level = std::numeric_limits<int>::min();
//  std::string file_name = GetFileName(f_name);
//  std::cout << "file name is " << file_name << "\n";
//
//  if (argc > 3) {
//    compression_level = std::stoi(argv[3]);
//  }
//  std::cout << "compression " << comp
//            << " with level:" << compression_level << "\n";
//
////    read_Parquet2ArrowDisk(f_name, comp, compression_level);
//  Parquet2ArrowDict(f_name, comp, compression_level);
//
//  std::cout << "Parquet/Arrow Reading and Writing complete." << std::endl;
//  return 0;
//}
