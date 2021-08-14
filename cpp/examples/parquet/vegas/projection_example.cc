//
// Created by Chunwei Liu on 7/23/21.
//

#include <cstdint>
#include <cstring>
#include <gandiva/precompiled/types.h>
#include <cassert>
#include <algorithm>
#include <iostream>
#include <memory>
#include <chrono>
#include <parquet/file_reader.h>
#include <parquet/column_reader.h>
#include "arrow/table.h"
#include "v_util.h"
#include <fstream>
#include "arrow/type_fwd.h"
#include <arrow/api.h>



void ScanParquetFileWithIndices(std::string filename, int* array, int size) {

  std::unique_ptr<parquet::ParquetFileReader> parquet_reader =
          parquet::ParquetFileReader::OpenFile(filename, false);

  std::shared_ptr<parquet::ColumnReader> column_reader;
  int n_rowgroup = parquet_reader->metadata()->num_row_groups();
  int16_t definition_level;
  int16_t repetition_level;

  int32_t* res_array = new int32_t[size];
  int32_t value = 0;
  int n_row = parquet_reader->metadata()->num_rows();
  std::cout << "# of row: "<< n_row << std::endl;
  std::cout << "# of row group: "<< n_rowgroup<< std::endl;
  int idx = 0;
  int loc = array[idx];
  int row_cnt = 0;
  int pre_loc = 0;

  for (int i=0;i<n_rowgroup;i++){
    int cur_rows = parquet_reader->RowGroup(i)->metadata()->num_rows();
    if (row_cnt + cur_rows<loc+1){
      row_cnt+=cur_rows;
      continue;
    }
    std::cout << "row group: "<< i<< std::endl;
    column_reader = parquet_reader->RowGroup(i)->Column(1);
    parquet::Int32Reader* int32_reader =
            static_cast<parquet::Int32Reader*>(column_reader.get());

    int64_t  values_read = 0;
    pre_loc = row_cnt;
    // if didn't finish current row group.
    while (loc-row_cnt<cur_rows){
      // Read one value at a time. The number of rows read is returned. values_read
      // contains the number of non-null rows
      int32_reader->Skip(loc-pre_loc-1);

      int32_reader->ReadBatch(1, &definition_level, &repetition_level, &value, &values_read);
      res_array[idx]=value;
      idx++;
      if (idx==size){
        break;
      }

      pre_loc = loc;
      loc = array[idx];

    }
    if (idx==size){
      break;
    }
    row_cnt+=cur_rows;
  }
//  for (int i=0;i<size;i++){
//    std::cout << "value lookup for row "<<array[i]<<": "<< res_array[i] << std::endl;
//  }

}



void ScanArrowFeatherWithIndices(std::string f_name, int* array, int size){

  std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
  std::shared_ptr<arrow::Table> table;
  std::vector<int> vect{1};
  read_feather_column_to_table(f_name, &table, vect);
  std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
  auto load_arrow = std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count();
  std::cout << "time elapsed in loading arrow: "<< load_arrow << std::endl;


//  int64_t batch_size = properties().batch_size();

  std::chrono::steady_clock::time_point lbegin = std::chrono::steady_clock::now();
  auto n_chunk = table->column(0)->num_chunks();
  int n_rows =table->num_rows();
  int idx = 0;
  int loc = array[idx];
  int32_t* res_array = new int32_t[size];

  std::cout << "# of row: "<< n_rows << std::endl;
  assert(n_rows>loc);
  std::cout << "# of chunk: "<< n_chunk<< std::endl;
  int row_cnt = 0;
  int value = 0;
  for (int i=0;i<n_chunk;i++){
    int cur_rows=table->column(0)->chunk(i)->length();
//    std::cout << "\t\t---# rows of current data chunk: "<< cur_rows<< std::endl;
    if (row_cnt+cur_rows<loc+1){
      row_cnt+=cur_rows;
      continue;
    }
    auto target_col =
            std::static_pointer_cast<arrow::Int32Array>(table->column(0)->chunk(i));

    // if didn't finish current row group.
    while (loc-row_cnt<cur_rows){
      value = target_col->Value(loc-row_cnt);
      res_array[idx]=value;
      idx++;
      if (idx==size){
        break;
      }
      loc = array[idx];
    }

    if (idx==size){
      break;
    }
    row_cnt+=cur_rows;
  }

  std::chrono::steady_clock::time_point lend = std::chrono::steady_clock::now();
  auto lookup_arrow = std::chrono::duration_cast<std::chrono::milliseconds>(lend - lbegin).count();
  std::cout << "time elapsed in lookup arrow: "<< lookup_arrow << std::endl;
//
//  for (int i=0;i<size;i++){
//    std::cout << "value lookup for row "<<array[i]<<": "<< res_array[i] << std::endl;
//  }
}







void ScanArrowFeatherSkip(std::string f_name, int* array, int size){


  std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
  int* converted_idx=new int[size];
  std::set<int> extracted_chunks = extractChunks(array,size,converted_idx);
  std::vector<int> chunks(extracted_chunks.size());
  std::copy(extracted_chunks.begin(), extracted_chunks.end(),chunks.begin());

  std::cout<<"chunk size:"<<chunks.size()<<std::endl;


  std::shared_ptr<arrow::Table> table;
  std::vector<int> vect{1};
  read_feather_column_to_table(f_name, &table, vect, chunks);
  std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
  auto load_arrow = std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count();
  std::cout << "time elapsed in loading arrow with skipping: "<< load_arrow << std::endl;


//  int64_t batch_size = properties().batch_size();

  std::chrono::steady_clock::time_point lbegin = std::chrono::steady_clock::now();
  auto n_chunk = table->column(0)->num_chunks();
  int n_rows =table->num_rows();
  int idx = 0;
  int loc = converted_idx[idx];
  int32_t* res_array = new int32_t[size];

  std::cout << "# of row: "<< n_rows << std::endl;
  assert(n_rows>loc);
  std::cout << "# of chunk: "<< n_chunk<< std::endl;
  int row_cnt = 0;
  int value = 0;
  for (int i=0;i<n_chunk;i++){
    int cur_rows=table->column(0)->chunk(i)->length();
//    std::cout << "\t\t---# rows of current data chunk: "<< cur_rows<< std::endl;
    if (row_cnt+cur_rows<loc+1){
      row_cnt+=cur_rows;
      continue;
    }
    auto target_col =
            std::static_pointer_cast<arrow::Int32Array>(table->column(0)->chunk(i));

    // if didn't finish current row group.
    while (loc-row_cnt<cur_rows){
      value = target_col->Value(loc-row_cnt);
      res_array[idx]=value;
      idx++;
      if (idx==size){
        break;
      }
      loc = converted_idx[idx];
    }

    if (idx==size){
      break;
    }
    row_cnt+=cur_rows;
  }

  std::chrono::steady_clock::time_point lend = std::chrono::steady_clock::now();
  auto lookup_arrow = std::chrono::duration_cast<std::chrono::milliseconds>(lend - lbegin).count();
  std::cout << "time elapsed in lookup arrow: "<< lookup_arrow << std::endl;

//  for (int i=0;i<size;i++){
//    std::cout << "value lookup for row "<<array[i]<<": "<< res_array[i] << std::endl;
//  }
}




int main(int argc, char** argv) {
  std::cout << "You have entered " << argc
            << " arguments:" << "\n";

  for (int i = 0; i < argc; ++i)
    std::cout << argv[i] << "\n";

  std::string f_name = argv[1];
  std::string comp = argv[2];
  float ratio = std::stof(argv[3]);
  int num=14401261;
//  int num=1441548;
  std::cout << "lookup with ratio "<<ratio<<" on "<<f_name<<" with " <<comp << "\n";

  int *indices = getRandomIdx(num,ratio);

  std::cout << "----Lookup parquet" << std::endl;
  auto begin = std::chrono::steady_clock::now();
  ScanParquetFileWithIndices(Get_Parquet_File(f_name,comp),indices, num*ratio);
  auto end = std::chrono::steady_clock::now();
  auto t_p_l = std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count();

  ScanArrowFeatherWithIndices(Get_Arrow_File(f_name,comp),indices, num*ratio);

  std::cout << "----Lookup arrow" << std::endl;
  auto a_begin = std::chrono::steady_clock::now();
  ScanArrowFeatherWithIndices(Get_Arrow_File(f_name,comp),indices, num*ratio);
  auto a_end = std::chrono::steady_clock::now();
  auto t_a_l = std::chrono::duration_cast<std::chrono::milliseconds>(a_end - a_begin).count();


  std::cout << "----Lookup arrow with skip" << std::endl;
  auto as_begin = std::chrono::steady_clock::now();
  ScanArrowFeatherSkip(Get_Arrow_File(f_name,comp),indices, num*ratio);
  auto as_end = std::chrono::steady_clock::now();
  auto t_as_l = std::chrono::duration_cast<std::chrono::milliseconds>(as_end - as_begin).count();

  std::cout << "runtime: "<< num*ratio <<","<<t_p_l<<","<<t_a_l << ","<<t_as_l << std::endl;

  return 0;
}

