//
// Created by Chunwei Liu on 8/28/21.
//

#include "arrow_helper.h"
#include "parquet_helper.h"
#include <arrow/array.h>
#include <arrow/type_fwd.h>
#include <arrow/chunked_array.h>

int main(){

  std::string f_name = "/mnt/dataset/catalog_sales";
  std::string comp = "uncompressed";


  std::unique_ptr<parquet::ParquetFileReader> parquet_reader =
          parquet::ParquetFileReader::OpenFile(Get_Parquet_File(f_name,comp), false);



  std::vector<int> projs;
  projs.push_back(2);
  projs.push_back(3);
  std::vector<int> filters;
  filters.push_back(1);
  filters.push_back(0);
  std::cout << "start arrow scan-fitler: " << projs.size() << std::endl;
  std::vector<std::string> ops;
  ops.push_back("EQUAL");
  ops.push_back("EQUAL");

  std::vector<std::string> opands;
  opands.push_back("12032");
  opands.push_back("2452653");

  std::cout << "start arrow scan-fitler: " << opands.size() << std::endl;
  auto begin = std::chrono::steady_clock::now();
  auto imme = ScanArrowTable(Get_Arrow_File(f_name,comp),  &projs, &filters, &ops, &opands);
  auto end = std::chrono::steady_clock::now();
  auto time_arrow = std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count();

  auto res_col = std::static_pointer_cast<arrow::Int32Array>(imme.results().at(0));
  auto res_col1 = std::static_pointer_cast<arrow::Int32Array>(imme.results().at(1));
  std::cout << "number of cols in result: " << imme.results().size() <<"length: "<< res_col->length()<<" and first val:"<< res_col->Value(0)<<" "<<res_col1->Value(0)<<std::endl;

  std::cout << "start parquet scan-fitler: " << std::endl;
  begin = std::chrono::steady_clock::now();
  auto imme1 = ScanParquetFile(Get_Parquet_File(f_name,comp),  &projs, &filters, &ops, &opands);
  end = std::chrono::steady_clock::now();
  auto time_parquet = std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count();
  res_col = std::static_pointer_cast<arrow::Int32Array>(imme1.results().at(0));
  res_col1 = std::static_pointer_cast<arrow::Int32Array>(imme1.results().at(1));
  std::cout << "number of cols in result: " << imme1.results().size() <<"length: "<< res_col->length()<<" and first val:"<< res_col->Value(0)<<" "<<res_col1->Value(0)<<std::endl;

  std::cout << "Query run time arrow parquet: " << time_arrow<<","<< time_parquet<<std::endl;

}