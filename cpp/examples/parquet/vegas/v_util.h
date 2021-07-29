//
// Created by Chunwei Liu on 7/27/21.
//

#include <arrow/filesystem/localfs.h>
#include <arrow/ipc/api.h>

#ifndef ARROW_V_UTIL_H
#define ARROW_V_UTIL_H

#endif //ARROW_V_UTIL_H

void read_feather_column_to_table(std::string path,std::shared_ptr<arrow::Table> *feather_table, std::vector<int>& indices){

  arrow::fs::LocalFileSystem file_system;
  std::shared_ptr <arrow::io::RandomAccessFile> input_file = file_system.OpenInputFile(path).ValueOrDie();
  std::shared_ptr <arrow::ipc::feather::Reader> feather_reader = arrow::ipc::feather::Reader::Open(input_file).ValueOrDie();
  arrow::Status temp_status = feather_reader -> Read(indices, feather_table);
  if(temp_status.ok()){
    std::cout << "Read feather file Successfully." << std::endl;
//    std::cout << ((*feather_table)->schema()) -> ToString() << std::endl; // this line gives segfault
  }
  else{
    std::cout << "Feather file reading process failed." << std::endl;
  }
  return;
}

std::string Get_Parquet_File(std::string f_name, std::string comp) {
  return f_name+"_"+comp+".parquet";
}


std::string Get_Arrow_File(std::string f_name, std::string comp) {
  return f_name+"_"+comp+".arrow";
}

// random generator function:
int myrandom (int i) { return std::rand()%i;}