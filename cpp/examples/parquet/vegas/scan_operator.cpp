//
// Created by Chunwei Liu on 8/28/21.
//

#include "arrow_helper.h"
#include "parquet_helper.h"
#include <arrow/array.h>
#include <arrow/type_fwd.h>
#include <arrow/chunked_array.h>

int main(){

  std::string comp = "zstd";
  std::string suf = "1";

  // start q1
  std::cout << "============Starting query 1 =============="<< std::endl;
  std::string f_name = "/mnt/dataset/catalog_sales"+suf;
  std::cout << f_name << std::endl;

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


  std::cout << "start parquet to arrow table and then scan-fitler: " << std::endl;
  begin = std::chrono::steady_clock::now();
  auto immept = ScanParquetTable(Get_Parquet_File(f_name,comp),  &projs, &filters, &ops, &opands);
  end = std::chrono::steady_clock::now();
  auto stime_pt = std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count();
  auto p_coli = std::static_pointer_cast<arrow::Int32Array>(immept.results().at(0));
  std::cout << "number of cols in result: " << immept.results().size() <<"length: "<< p_coli->length()<<" and first val:"<< p_coli->Value(0)<<std::endl;

  std::cout << "Query run time arrow parquet parquet-table: " << time_arrow<<","<< time_parquet<<","<<stime_pt<<std::endl;


  // start q2
  std::cout << "============Starting query 2 =============="<<std::endl;
  f_name = "/mnt/dataset/customer_demographics_cp"+suf;

  std::vector<int> sprojs;
  sprojs.push_back(0);
  sprojs.push_back(8);
  std::vector<int> sfilters;
  sfilters.push_back(1);
  sfilters.push_back(3);
  std::cout << "start arrow scan-fitler: " << sprojs.size() << std::endl;
  std::vector<std::string> sops;
  sops.push_back("EQUAL");
  sops.push_back("EQUAL");

  std::vector<std::string> sopands;
  sopands.push_back("F");
  sopands.push_back("Secondary");

  std::cout << "start arrow scan-fitler: " << sopands.size() << std::endl;
  begin = std::chrono::steady_clock::now();
  auto simme = ScanArrowTable(Get_Arrow_File(f_name,comp),  &sprojs, &sfilters, &sops, &sopands);
  end = std::chrono::steady_clock::now();
  time_arrow = std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count();

  auto sres_col = std::static_pointer_cast<arrow::Int32Array>(simme.results().at(0));
  auto sres_col1 = std::static_pointer_cast<arrow::Int32Array>(simme.results().at(1));
  std::cout << "number of cols in result: " << simme.results().size() <<"length: "<< sres_col->length()<<" and first val:"<< sres_col->Value(0)<<" "<<sres_col1->Value(0)<<std::endl;

  std::cout << "start parquet scan-fitler: " << std::endl;
  begin = std::chrono::steady_clock::now();
  auto simme1 = ScanParquetFile(Get_Parquet_File(f_name,comp),  &sprojs, &sfilters, &sops, &sopands);
  end = std::chrono::steady_clock::now();
  auto stime_parquet = std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count();
  res_col = std::static_pointer_cast<arrow::Int32Array>(simme1.results().at(0));
  res_col1 = std::static_pointer_cast<arrow::Int32Array>(simme1.results().at(1));
  std::cout << "number of cols in result: " << simme1.results().size() <<"length: "<< sres_col->length()<<" and first val:"<< sres_col->Value(0)<<" "<<sres_col1->Value(0)<<std::endl;


  std::cout << "start parquet to arrow table and then scan-fitler: " << std::endl;
  begin = std::chrono::steady_clock::now();
  immept = ScanParquetTable(Get_Parquet_File(f_name,comp),  &sprojs, &sfilters, &sops, &sopands);
  end = std::chrono::steady_clock::now();
  stime_pt = std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count();
  p_coli= std::static_pointer_cast<arrow::Int32Array>(immept.results().at(0));
  std::cout << "number of cols in result: " << immept.results().size() <<"length: "<< p_coli->length()<<" and first val:"<< p_coli->Value(0)<<std::endl;

  std::cout << "Query run time arrow parquet parquet-table: " << time_arrow<<","<< stime_parquet<<","<<stime_pt<<std::endl;




  // start q3
  std::cout << "============Starting query 3 =============="<<std::endl;
  f_name = "/mnt/dataset/customer_demographics_cp"+suf;

  sprojs.clear();
  sprojs.push_back(0);
  sfilters.clear();
  sfilters.push_back(1);
  sfilters.push_back(2);
  sfilters.push_back(3);
  std::cout << "start arrow scan-fitler: " << sprojs.size() << std::endl;
  sops.clear();
  sops.push_back("EQUAL");
  sops.push_back("EQUAL");
  sops.push_back("EQUAL");

  sopands.clear();
  sopands.push_back("M");
  sopands.push_back("D");
  sopands.push_back("College");

  std::cout << "start arrow scan-fitler: " << sopands.size() << std::endl;
  begin = std::chrono::steady_clock::now();
  simme = ScanArrowTable(Get_Arrow_File(f_name,comp),  &sprojs, &sfilters, &sops, &sopands);
  end = std::chrono::steady_clock::now();
  time_arrow = std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count();

  sres_col = std::static_pointer_cast<arrow::Int32Array>(simme.results().at(0));
//  sres_col1 = std::static_pointer_cast<arrow::Int32Array>(simme.results().at(1));
  std::cout << "number of cols in result: " << simme.results().size() <<"length: "<< sres_col->length()<<" and first val:"<< sres_col->Value(0)<<std::endl;

  std::cout << "start parquet scan-fitler: " << std::endl;
  begin = std::chrono::steady_clock::now();
  simme1 = ScanParquetFile(Get_Parquet_File(f_name,comp),  &sprojs, &sfilters, &sops, &sopands);
  end = std::chrono::steady_clock::now();
  stime_parquet = std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count();
  res_col = std::static_pointer_cast<arrow::Int32Array>(simme1.results().at(0));
  std::cout << "number of cols in result: " << simme1.results().size() <<"length: "<< sres_col->length()<<" and first val:"<< sres_col->Value(0)<<std::endl;

  std::cout << "start parquet to arrow table and then scan-fitler: " << std::endl;
  begin = std::chrono::steady_clock::now();
  immept = ScanParquetTable(Get_Parquet_File(f_name,comp),  &sprojs, &sfilters, &sops, &sopands);
  end = std::chrono::steady_clock::now();
  stime_pt = std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count();
  p_coli = std::static_pointer_cast<arrow::Int32Array>(immept.results().at(0));
  std::cout << "number of cols in result: " << immept.results().size() <<"length: "<< p_coli->length()<<" and first val:"<< p_coli->Value(0)<<std::endl;

  std::cout << "Query run time arrow parquet parquet-table: " << time_arrow<<","<< stime_parquet<<","<<stime_pt<<std::endl;




  // start q4
  std::cout << "============Starting query 4 =============="<<std::endl;
  f_name = "/mnt/dataset/catalog_sales"+suf;

  sprojs.clear();
  sprojs.push_back(23);
  sprojs.push_back(0);
  sprojs.push_back(15);
  sfilters.clear();
  sfilters.push_back(19);
  sfilters.push_back(26);
  std::cout << "start arrow scan-fitler: " << sprojs.size() << std::endl;
  sops.clear();
  sops.push_back("GREATER");
  sops.push_back("LESS");

  sopands.clear();
  sopands.push_back("80.0");
  sopands.push_back("500.0");

  std::cout << "start arrow scan-fitler: " << sopands.size() << std::endl;
  begin = std::chrono::steady_clock::now();
  simme = ScanArrowTable(Get_Arrow_File(f_name,comp),  &sprojs, &sfilters, &sops, &sopands);
  end = std::chrono::steady_clock::now();
  time_arrow = std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count();

  auto n_col = std::static_pointer_cast<arrow::DoubleArray>(simme.results().at(0));
//  sres_col1 = std::static_pointer_cast<arrow::Int32Array>(simme.results().at(1));
  std::cout << "number of cols in result: " << simme.results().size() <<"length: "<< n_col->length()<<" and first val:"<< n_col->Value(1)<<std::endl;

  std::cout << "start parquet scan-fitler: " << std::endl;
  begin = std::chrono::steady_clock::now();
  simme1 = ScanParquetFile(Get_Parquet_File(f_name,comp),  &sprojs, &sfilters, &sops, &sopands);
  end = std::chrono::steady_clock::now();
  stime_parquet = std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count();
  auto p_col = std::static_pointer_cast<arrow::DoubleArray>(simme1.results().at(0));
  std::cout << "number of cols in result: " << simme1.results().size() <<"length: "<< p_col->length()<<" and first val:"<< p_col->Value(1)<<std::endl;

  std::cout << "start parquet to arrow table and then scan-fitler: " << std::endl;
  begin = std::chrono::steady_clock::now();
  immept = ScanParquetTable(Get_Parquet_File(f_name,comp),  &sprojs, &sfilters, &sops, &sopands);
  end = std::chrono::steady_clock::now();
  stime_pt = std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count();
  p_col = std::static_pointer_cast<arrow::DoubleArray>(immept.results().at(0));
  std::cout << "number of cols in result: " << immept.results().size() <<"length: "<< p_col->length()<<" and first val:"<< p_col->Value(1)<<std::endl;

  std::cout << "Query run time arrow parquet parquet-table: " << time_arrow<<","<< stime_parquet<<","<<stime_pt<<std::endl;



  // start q5
  std::cout << "============Starting query 5 =============="<<std::endl;
  f_name = "/mnt/dataset/catalog_sales"+suf;

  sprojs.clear();
  sprojs.push_back(23);
  sprojs.push_back(0);
  sprojs.push_back(15);
  sprojs.push_back(29);
  sprojs.push_back(32);
  sprojs.push_back(33);
  sfilters.clear();
  sfilters.push_back(19);
  std::cout << "start arrow scan-fitler: " << sprojs.size() << std::endl;
  sops.clear();
  sops.push_back("GREATER");

  sopands.clear();
  sopands.push_back("80.0");


  std::cout << "start arrow scan-fitler: " << sopands.size() << std::endl;
  begin = std::chrono::steady_clock::now();
  simme = ScanArrowTable(Get_Arrow_File(f_name,comp),  &sprojs, &sfilters, &sops, &sopands);
  end = std::chrono::steady_clock::now();
  time_arrow = std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count();

  n_col = std::static_pointer_cast<arrow::DoubleArray>(simme.results().at(0));
//  sres_col1 = std::static_pointer_cast<arrow::Int32Array>(simme.results().at(1));
  std::cout << "number of cols in result: " << simme.results().size() <<"length: "<< n_col->length()<<" and first val:"<< n_col->Value(1)<<std::endl;

  std::cout << "start parquet scan-fitler: " << std::endl;
  begin = std::chrono::steady_clock::now();
  simme1 = ScanParquetFile(Get_Parquet_File(f_name,comp),  &sprojs, &sfilters, &sops, &sopands);
  end = std::chrono::steady_clock::now();
  stime_parquet = std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count();
  p_col = std::static_pointer_cast<arrow::DoubleArray>(simme1.results().at(0));
  std::cout << "number of cols in result: " << simme1.results().size() <<"length: "<< p_col->length()<<" and first val:"<< p_col->Value(1)<<std::endl;


  std::cout << "start parquet to arrow table and then scan-fitler: " << std::endl;
  begin = std::chrono::steady_clock::now();
  immept = ScanParquetTable(Get_Parquet_File(f_name,comp),  &sprojs, &sfilters, &sops, &sopands);
  end = std::chrono::steady_clock::now();
  stime_pt = std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count();
  p_col = std::static_pointer_cast<arrow::DoubleArray>(immept.results().at(0));
  std::cout << "number of cols in result: " << immept.results().size() <<"length: "<< p_col->length()<<" and first val:"<< p_col->Value(1)<<std::endl;

  std::cout << "Query run time arrow parquet parquet-table: " << time_arrow<<","<< stime_parquet<<","<<stime_pt<<std::endl;





}