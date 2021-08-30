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


  parquet::ByteArray value;
  int n_row = parquet_reader->metadata()->num_rows();
  std::cout << "# of row: "<< n_row << std::endl;
  std::cout << "# of row group: "<< n_rowgroup<< std::endl;
  int idx = 0;
  int loc = array[idx];
  int row_cnt = 0;
  int pre_loc = 0;
  int qualified = 0;
  std::string pred = "Primary";
  parquet::ByteArray target = parquet::ByteArray(pred.length(), reinterpret_cast<const uint8_t *>(pred.data()));

  for (int i=0;i<n_rowgroup;i++){
    int cur_rows = parquet_reader->RowGroup(i)->metadata()->num_rows();
    if (row_cnt + cur_rows<loc+1){
      row_cnt+=cur_rows;
      continue;
    }
    std::cout << "row group: "<< i<< std::endl;
    column_reader = parquet_reader->RowGroup(i)->Column(3);
    parquet::ByteArrayReader* int32_reader =
            static_cast<parquet::ByteArrayReader*>(column_reader.get());

    int64_t  values_read = 0;
    pre_loc = row_cnt;
    // if didn't finish current row group.
    while (loc-row_cnt<cur_rows){
      // Read one value at a time. The number of rows read is returned. values_read
      // contains the number of non-null rows
      int32_reader->Skip(loc-pre_loc-1);

      int32_reader->ReadBatch(1, &definition_level, &repetition_level, &value, &values_read);
      if (value==target){
        qualified++;
      }
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

  std::cout << "Number of qualified: "<< qualified << std::endl;

}

void FilterParquetFileOnString(std::string filename, std::string pred = "Primary") {

  std::unique_ptr<parquet::ParquetFileReader> parquet_reader =
          parquet::ParquetFileReader::OpenFile(filename, false);

  std::shared_ptr<parquet::ColumnReader> column_reader;
  int n_rowgroup = parquet_reader->metadata()->num_row_groups();
  int16_t definition_level;
  int16_t repetition_level;


  parquet::ByteArray value;
  int n_row = parquet_reader->metadata()->num_rows();
  std::cout << "# of row: "<< n_row << std::endl;
  std::cout << "# of row group: "<< n_rowgroup<< std::endl;
  int qualified = 0;
  parquet::ByteArray target = parquet::ByteArray(pred.length(), reinterpret_cast<const uint8_t *>(pred.data()));

  for (int i=0;i<n_rowgroup;i++){
    int cur_rows = parquet_reader->RowGroup(i)->metadata()->num_rows();
    std::cout << "row group: "<< i<< std::endl;
    column_reader = parquet_reader->RowGroup(i)->Column(3);
    parquet::ByteArrayReader* int32_reader =
            static_cast<parquet::ByteArrayReader*>(column_reader.get());

    int64_t  values_read = 0;
    // if didn't finish current row group.
    for (int j =0;j<cur_rows;j++){
      // Read one value at a time. The number of rows read is returned. values_read
      // contains the number of non-null rows
      int32_reader->ReadBatch(1, &definition_level, &repetition_level, &value, &values_read);
      if (value==target){
        qualified++;
      }
    }

  }

  std::cout << "Number of qualified: "<< qualified << std::endl;

}


void ScanArrowFeatherWithIndices(std::string f_name, int* array, int size){

  std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
  std::shared_ptr<arrow::Table> table;
  std::vector<int> vect{3};
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





void ScanArrowFeatherWithIndicesOnStringDecoded(std::string f_name, int* array, int size){

  std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
  std::shared_ptr<arrow::Table> table;
  std::vector<int> vect{3};
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
  std::string* res_array = new std::string[size];

  std::cout << "# of row: "<< n_rows << std::endl;
  assert(n_rows>loc);
  std::cout << "# of chunk: "<< n_chunk<< std::endl;
  int row_cnt = 0;
  std::string value = "";
  for (int i=0;i<n_chunk;i++){
    int cur_rows=table->column(0)->chunk(i)->length();
//    std::cout << "\t\t---# rows of current data chunk: "<< cur_rows<< std::endl;
    if (row_cnt+cur_rows<loc+1){
      row_cnt+=cur_rows;
      continue;
    }
    auto target_col =
            std::static_pointer_cast<arrow::DictionaryArray>(table->column(0)->chunk(i));
    auto dict = std::static_pointer_cast<arrow::StringArray>(target_col->dictionary());
//    std::cout << "\t\t---print our cur "<< dict->GetString(1) <<" value: "<<std::endl;

    // if didn't finish current row group.
    while (loc-row_cnt<cur_rows){
      value = dict->GetString(target_col->GetValueIndex(loc-row_cnt));
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
//  for (int i=0;i<10;i++){
//    std::cout << "value lookup for row "<<array[i]<<": "<< res_array[i] << std::endl;
//  }
}


void ScanArrowFeatherWithIndicesOnString(std::string f_name, int* array, int size){

  std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
  std::shared_ptr<arrow::Table> table;
  std::vector<int> vect{3};
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
  int* res_array = new int[size];

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
            std::static_pointer_cast<arrow::DictionaryArray>(table->column(0)->chunk(i));
    auto dict = std::static_pointer_cast<arrow::StringArray>(target_col->dictionary());
//    std::cout << "\t\t---print our cur "<< dict->GetString(1) <<" value: "<<std::endl;

    // if didn't finish current row group.
    while (loc-row_cnt<cur_rows){
      value = target_col->GetValueIndex(loc-row_cnt);
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
//  for (int i=0;i<10;i++){
//    std::cout << "value lookup for row "<<array[i]<<": "<< res_array[i] << std::endl;
//  }
}

void filteringArrowFeatherWithIndicesOnStringDecoded(std::string f_name, int* array, int size){

  std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
  std::shared_ptr<arrow::Table> table;
  std::vector<int> vect{3};
  read_feather_column_to_table(f_name, &table, vect);
  std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
  auto load_arrow = std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count();
  std::cout << "time elapsed in loading arrow: "<< load_arrow << std::endl;

  std::string pred = "Primary";
//  int64_t batch_size = properties().batch_size();

  std::chrono::steady_clock::time_point lbegin = std::chrono::steady_clock::now();
  auto n_chunk = table->column(0)->num_chunks();
  int n_rows =table->num_rows();
  int idx = 0;
  int loc = array[idx];


  std::cout << "# of row: "<< n_rows << std::endl;
  assert(n_rows>loc);
  std::cout << "# of chunk: "<< n_chunk<< std::endl;
  int row_cnt = 0;
  std::string value = "";
  int qualified = 0;

  for (int i=0;i<n_chunk;i++){
    int cur_rows=table->column(0)->chunk(i)->length();
//    std::cout << "\t\t---# rows of current data chunk: "<< cur_rows<< std::endl;
    if (row_cnt+cur_rows<loc+1){
      row_cnt+=cur_rows;
      continue;
    }
    auto target_col =
            std::static_pointer_cast<arrow::DictionaryArray>(table->column(0)->chunk(i));
    auto dict = std::static_pointer_cast<arrow::StringArray>(target_col->dictionary());
//    std::cout << "\t\t---print our cur "<< dict->GetString(1) <<" value: "<<std::endl;

    // if didn't finish current row group.
    while (loc-row_cnt<cur_rows){
      value = dict->GetString(target_col->GetValueIndex(loc-row_cnt));
      if (value==pred){
        qualified++;
      };
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
  std::cout << "Number of qualified: "<< qualified << std::endl;
  std::cout << "time elapsed in lookup arrow: "<< lookup_arrow << std::endl;

}

void filteringArrowFeatherOnStringDecoded(std::string f_name, std::string pred = "Primary"){

  std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
  std::shared_ptr<arrow::Table> table;
  std::vector<int> vect{3};
  read_feather_column_to_table(f_name, &table, vect);
  std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
  auto load_arrow = std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count();
  std::cout << "time elapsed in loading arrow: "<< load_arrow << std::endl;

//  int64_t batch_size = properties().batch_size();

  std::chrono::steady_clock::time_point lbegin = std::chrono::steady_clock::now();
  auto n_chunk = table->column(0)->num_chunks();
  int n_rows =table->num_rows();

  std::cout << "# of row: "<< n_rows << std::endl;

  std::cout << "# of chunk: "<< n_chunk<< std::endl;
  std::string value = "";
  int qualified = 0;

  for (int i=0;i<n_chunk;i++){
    int cur_rows=table->column(0)->chunk(i)->length();
    auto target_col =
            std::static_pointer_cast<arrow::DictionaryArray>(table->column(0)->chunk(i));
    auto dict = std::static_pointer_cast<arrow::StringArray>(target_col->dictionary());

    for (int j = 0; j<cur_rows;j++){
      value = dict->GetString(target_col->GetValueIndex(j));
      if (value==pred){
        qualified++;
      };
    }
  }

  std::chrono::steady_clock::time_point lend = std::chrono::steady_clock::now();
  auto lookup_arrow = std::chrono::duration_cast<std::chrono::milliseconds>(lend - lbegin).count();
  std::cout << "Number of qualified: "<< qualified << std::endl;
  std::cout << "time elapsed in lookup arrow: "<< lookup_arrow << std::endl;

}


void filterArrowFeatherWithIndicesOnString(std::string f_name, int* array, int size){

  std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
  std::shared_ptr<arrow::Table> table;
  std::vector<int> vect{3};
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

  int qualified = 0;
  int pred = 0;

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
            std::static_pointer_cast<arrow::DictionaryArray>(table->column(0)->chunk(i));
    auto dict = std::static_pointer_cast<arrow::StringArray>(target_col->dictionary());
//    std::cout << "\t\t---print our cur "<< dict->GetString(1) <<" value: "<<std::endl;

    // if didn't finish current row group.
    while (loc-row_cnt<cur_rows){
      value = target_col->GetValueIndex(loc-row_cnt);
      if (value==pred){
        qualified++;
      }
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
  std::cout << "Number of qualified: "<< qualified << std::endl;
  std::cout << "time elapsed in lookup arrow: "<< lookup_arrow << std::endl;
//
//  for (int i=0;i<10;i++){
//    std::cout << "value lookup for row "<<array[i]<<": "<< res_array[i] << std::endl;
//  }
}

void filterArrowFeatherOnString(std::string f_name, std::string predicate = "Primary"){

  std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
  std::shared_ptr<arrow::Table> table;
  std::vector<int> vect{3};
  read_feather_column_to_table(f_name, &table, vect);
  std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
  auto load_arrow = std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count();
  std::cout << "time elapsed in loading arrow: "<< load_arrow << std::endl;


  std::chrono::steady_clock::time_point lbegin = std::chrono::steady_clock::now();
  auto n_chunk = table->column(0)->num_chunks();
  int n_rows =table->num_rows();
  std::cout<<"Type: "<< table->schema()->field(0)->type()->name()<<std::endl;

  int qualified = 0;
  int pred = 0;

  std::cout << "# of row: "<< n_rows << std::endl;
  assert(n_rows>loc);
  std::cout << "# of chunk: "<< n_chunk<< std::endl;
  int value = 0;
  for (int i=0;i<n_chunk;i++){
    int cur_rows=table->column(0)->chunk(i)->length();
    auto target_col =
            std::static_pointer_cast<arrow::DictionaryArray>(table->column(0)->chunk(i));
    auto dict = std::static_pointer_cast<arrow::StringArray>(target_col->dictionary());

    for (int j=0;j<cur_rows;j++){
      value = target_col->GetValueIndex(j);
      if (value==pred){
        qualified++;
      }

    }
  }

  std::chrono::steady_clock::time_point lend = std::chrono::steady_clock::now();
  auto lookup_arrow = std::chrono::duration_cast<std::chrono::milliseconds>(lend - lbegin).count();
  std::cout << "Number of qualified: "<< qualified << std::endl;
  std::cout << "time elapsed in lookup arrow: "<< lookup_arrow << std::endl;

}

void ScanArrowFeatherSkip(std::string f_name, int* array, int size){


  std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
  int* converted_idx=new int[size];
  std::set<int> extracted_chunks = extractChunks(array,size,converted_idx);
  std::vector<int> chunks(extracted_chunks.size());
  std::copy(extracted_chunks.begin(), extracted_chunks.end(),chunks.begin());

  std::cout<<"chunk size:"<<chunks.size()<<std::endl;


  std::shared_ptr<arrow::Table> table;
  std::vector<int> vect{3};
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
  std::string* res_array = new std::string[size];

  std::cout << "# of row: "<< n_rows << std::endl;
  assert(n_rows>loc);
  std::cout << "# of chunk: "<< n_chunk<< std::endl;
  int row_cnt = 0;
  std::string value = 0;
  for (int i=0;i<n_chunk;i++){
    int cur_rows=table->column(0)->chunk(i)->length();
//    std::cout << "\t\t---# rows of current data chunk: "<< cur_rows<< std::endl;
    if (row_cnt+cur_rows<loc+1){
      row_cnt+=cur_rows;
      continue;
    }
    auto target_col =
            std::static_pointer_cast<arrow::StringArray>(table->column(0)->chunk(i));

    // if didn't finish current row group.
    while (loc-row_cnt<cur_rows){
      value = target_col->GetString(loc-row_cnt);
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

void FilterArrowFeather(std::string f_name,std::string predicate = "Primary"){

  std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
  std::shared_ptr<arrow::Table> table;
  std::vector<int> vect{3};
  read_feather_column_to_table(f_name, &table, vect);
  std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
  auto load_arrow = std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count();
  std::cout << "time elapsed in loading arrow with skipping: "<< load_arrow << std::endl;


//  int64_t batch_size = properties().batch_size();

  std::chrono::steady_clock::time_point lbegin = std::chrono::steady_clock::now();
  auto n_chunk = table->column(0)->num_chunks();
  std::cout<<"Type: "<< table->schema()->field(0)->type()->name()<<std::endl;
  int n_rows =table->num_rows();
  std::string value = "";
  int qualified = 0;

  std::cout << "# of chunk: "<< n_chunk<< std::endl;
  for (int i=0;i<n_chunk;i++){
    int cur_rows=table->column(0)->chunk(i)->length();
    auto target_col =
            std::static_pointer_cast<arrow::StringArray>(table->column(0)->chunk(i));

    // if didn't finish current row group.
    for (int j=0;j<cur_rows;j++){
      value = target_col->GetString(j);
      if (value == predicate){
        qualified++;
      };
    }

  }

  std::chrono::steady_clock::time_point lend = std::chrono::steady_clock::now();
  auto lookup_arrow = std::chrono::duration_cast<std::chrono::milliseconds>(lend - lbegin).count();
  std::cout << "Number of qualified: "<< qualified << std::endl;
  std::cout << "time elapsed in lookup arrow: "<< lookup_arrow << std::endl;
}





int main(int argc, char** argv) {
  std::cout << "You have entered " << argc
            << " arguments:" << "\n";

  for (int i = 0; i < argc; ++i)
    std::cout << argv[i] << "\n";

  std::string f_name = argv[1];
  std::string comp = argv[2];
  int num=14401261;
//  int num=1441548;
  std::cout << "filtering on "<<f_name<<" with " <<comp << "\n";

  std::cout << "----Lookup parquet" << std::endl;
  auto begin = std::chrono::steady_clock::now();
  FilterParquetFileOnString(Get_Parquet_File(f_name,comp));
  auto end = std::chrono::steady_clock::now();
  auto t_p_l = std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count();

//  ScanArrowFeatherWithIndices(Get_Arrow_File(f_name,comp),indices, num*ratio);

  std::cout << "----filtering arrow with dictionary decoded" << std::endl;
  auto a_begin = std::chrono::steady_clock::now();
  if (f_name.find("_cp") != std::string::npos){
    filteringArrowFeatherOnStringDecoded(Get_Arrow_File(f_name,comp));
  }
  auto a_end = std::chrono::steady_clock::now();
  auto t_a_l = std::chrono::duration_cast<std::chrono::milliseconds>(a_end - a_begin).count();

  std::cout << "----filtering arrow without dictionary decoding" << std::endl;
  auto ad_begin = std::chrono::steady_clock::now();
  if (f_name.find("_cp") != std::string::npos){
    filterArrowFeatherOnString(Get_Arrow_File(f_name,comp));
  }
  auto ad_end = std::chrono::steady_clock::now();
  auto t_ad_l = std::chrono::duration_cast<std::chrono::milliseconds>(ad_end - ad_begin).count();


  std::cout << "----fitlering arrow on string chunk" << std::endl;
  auto as_begin = std::chrono::steady_clock::now();
  if (f_name.find("_cp") == std::string::npos)
  {
    FilterArrowFeather(Get_Arrow_File(f_name,comp));
  }
  auto as_end = std::chrono::steady_clock::now();
  auto t_as_l = std::chrono::duration_cast<std::chrono::milliseconds>(as_end - as_begin).count();

  std::cout << "runtime: "<<t_p_l<<","<<t_a_l << ","<<t_ad_l << ","<<t_as_l << std::endl;

  return 0;
}

