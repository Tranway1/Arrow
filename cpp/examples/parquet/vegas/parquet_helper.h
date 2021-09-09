//
// Created by Chunwei Liu on 8/25/21.
//

#include <parquet/file_reader.h>
#include <parquet/column_reader.h>

#ifndef ARROW_PARQUET_HELPER_H
#define ARROW_PARQUET_HELPER_H

#endif //ARROW_PARQUET_HELPER_H

inline bool ByteArrayEqual(parquet::ByteArray a, parquet::ByteArray b){
  return a==b;
}

template<class T>
IntermediateResult FilterParquetDouble(std::shared_ptr<parquet::ParquetFileReader> parquet_reader, int col,
                                       double pred, T func, std::shared_ptr<arrow::Array> pre = nullptr) {

  std::shared_ptr<parquet::ColumnReader> column_reader;
  std::shared_ptr<arrow::Array> array;

  int n_rowgroup = parquet_reader->metadata()->num_row_groups();

  // this is for null check
  int16_t definition_level;
  int16_t repetition_level;
  arrow::Int64Builder builder;

  int cnt = 0;

  if (pre == nullptr){
    // for each rou group, get col col chunk
    for (int i=0;i<n_rowgroup;i++){
      column_reader = parquet_reader->RowGroup(i)->Column(col);
      parquet::DoubleReader* double_reader =
              static_cast<parquet::DoubleReader *>(column_reader.get());

      int64_t  values_read = 0;

      while (double_reader->HasNext()) {
        double value;
        // Read one value at a time. The number of rows read is returned. values_read
        // contains the number of non-null rows
        double_reader->ReadBatch(1, &definition_level, &repetition_level, &value, &values_read);
        // There are no NULL values in the rows written
//        if (values_read == 0){
//          cnt++;
//          continue;
//        }

        if (func(value,pred)){
          builder.Append(cnt);
        }
        cnt++;
      }
    }
  }
  else if (pre->length()>0){
    auto bv = std::static_pointer_cast<arrow::Int64Array>(pre);
    int idx = 0;
    int loc = bv->Value(idx);
    int row_cnt = 0;
    int pre_loc = 0;
    double value = 0.0;
    auto size = pre->length();

    for (int i=0;i<n_rowgroup;i++){
      int cur_rows = parquet_reader->RowGroup(i)->metadata()->num_rows();
      if (row_cnt + cur_rows<loc+1){
        row_cnt+=cur_rows;
        continue;
      }
      std::cout << "row group: "<< i<< std::endl;
      column_reader = parquet_reader->RowGroup(i)->Column(col);
      parquet::DoubleReader* double_reader =
              static_cast<parquet::DoubleReader*>(column_reader.get());

      int64_t  values_read = 0;
      pre_loc = row_cnt;
      // if didn't finish current row group.
      while (loc-row_cnt<cur_rows){
        // Read one value at a time. The number of rows read is returned. values_read
        // contains the number of non-null rows
        double_reader->Skip(loc-pre_loc-1);

        double_reader->ReadBatch(1, &definition_level, &repetition_level, &value, &values_read);
        if (func(value,pred)){
          builder.Append(loc);
        }
        idx++;
        if (idx==size){
          break;
        }

        pre_loc = loc;
        loc = bv->Value(idx);;

      }
      if (idx==size){
        break;
      }
      row_cnt+=cur_rows;
    }
  }

  if(!builder.Finish(&array).ok())
    throw std::runtime_error("Could not create result array.");

  return IntermediateResult{array};
}

IntermediateResult ScanParquetDouble(std::shared_ptr<parquet::ParquetFileReader> parquet_reader, int col,
                                     std::shared_ptr<arrow::Array> pre = nullptr) {
  arrow::DoubleBuilder val_builder;
  std::shared_ptr<arrow::Array> val_array;
  std::shared_ptr<parquet::ColumnReader> column_reader;

  int n_rowgroup = parquet_reader->metadata()->num_row_groups();

  // this is for null check
  int16_t definition_level;
  int16_t repetition_level;

  if (pre == nullptr){
    // for each rou group, get col col chunk
    for (int i=0;i<n_rowgroup;i++){
      column_reader = parquet_reader->RowGroup(i)->Column(col);
      parquet::DoubleReader* double_reader =
              static_cast<parquet::DoubleReader *>(column_reader.get());

      int64_t  values_read = 0;

      while (double_reader->HasNext()) {
        double value;
        // Read one value at a time. The number of rows read is returned. values_read
        // contains the number of non-null rows
        double_reader->ReadBatch(1, &definition_level, &repetition_level, &value, &values_read);
        val_builder.Append(value);
      }
    }
  }
  else if (pre->length()>0){
    auto bv = std::static_pointer_cast<arrow::Int64Array>(pre);
    int idx = 0;
    int loc = bv->Value(idx);
    int row_cnt = 0;
    int pre_loc = 0;
    double value = 0.0;
    auto size = pre->length();

    for (int i=0;i<n_rowgroup;i++){
      int cur_rows = parquet_reader->RowGroup(i)->metadata()->num_rows();
      if (row_cnt + cur_rows<loc+1){
        row_cnt+=cur_rows;
        continue;
      }
      std::cout << "row group: "<< i<< std::endl;
      column_reader = parquet_reader->RowGroup(i)->Column(col);
      parquet::DoubleReader* double_reader =
              static_cast<parquet::DoubleReader*>(column_reader.get());

      int64_t  values_read = 0;
      pre_loc = row_cnt;
      // if didn't finish current row group.
      while (loc-row_cnt<cur_rows){
        // Read one value at a time. The number of rows read is returned. values_read
        // contains the number of non-null rows
        double_reader->Skip(loc-pre_loc-1);

        double_reader->ReadBatch(1, &definition_level, &repetition_level, &value, &values_read);
        val_builder.Append(value);
        idx++;
        if (idx==size){
          break;
        }

        pre_loc = loc;
        loc = bv->Value(idx);;

      }
      if (idx==size){
        break;
      }
      row_cnt+=cur_rows;
    }
  }

  if(!val_builder.Finish(&val_array).ok())
    throw std::runtime_error("Could not create result array.");
  std::vector<std::shared_ptr<arrow::Array>> res;
  res.push_back(val_array);
  auto im = IntermediateResult{res};
  return im;
}

template<class T>
IntermediateResult FilterParquetInt(std::shared_ptr<parquet::ParquetFileReader> parquet_reader, int col,
                                    int pred, T func, std::shared_ptr<arrow::Array> pre = nullptr) {

  std::shared_ptr<parquet::ColumnReader> column_reader;
  std::shared_ptr<arrow::Array> array;

  int n_rowgroup = parquet_reader->metadata()->num_row_groups();

  // this is for null check
  int16_t definition_level;
  int16_t repetition_level;
  arrow::Int64Builder builder;

  int cnt = 0;

  if (pre == nullptr){
    // for each rou group, get col col chunk
    for (int i=0;i<n_rowgroup;i++){
      column_reader = parquet_reader->RowGroup(i)->Column(col);
      parquet::Int32Reader* int_reader =
              static_cast<parquet::Int32Reader *>(column_reader.get());

      int64_t  values_read = 0;

      while (int_reader->HasNext()) {
        int value;
        // Read one value at a time. The number of rows read is returned. values_read
        int_reader->ReadBatch(1, &definition_level, &repetition_level, &value, &values_read);
        // There are no NULL values in the rows written
//        if (values_read == 0){
//          cnt++;
//          continue;
//        }
        if (func(value,pred)){
          builder.Append(cnt);
        }
        cnt++;
      }
    }
  }
  else if (pre->length()>0){
    auto bv = std::static_pointer_cast<arrow::Int64Array>(pre);
    int idx = 0;
    int loc = bv->Value(idx);
    int row_cnt = 0;
    int pre_loc = 0;
    int value = 0.0;
    auto size = pre->length();
    for (int i=0;i<n_rowgroup;i++){
      int cur_rows = parquet_reader->RowGroup(i)->metadata()->num_rows();
      if (row_cnt + cur_rows<loc+1){
        row_cnt+=cur_rows;
        continue;
      }
      std::cout << "row group: "<< i<< std::endl;
      column_reader = parquet_reader->RowGroup(i)->Column(col);
      parquet::Int32Reader* int_reader =
              static_cast<parquet::Int32Reader*>(column_reader.get());
      int64_t  values_read = 0;
      pre_loc = row_cnt;
      // if didn't finish current row group.
      while (loc-row_cnt<cur_rows){
        int_reader->Skip(loc-pre_loc-1);
        int_reader->ReadBatch(1, &definition_level, &repetition_level, &value, &values_read);
        if (func(value,pred)){
          builder.Append(loc);
        }
        idx++;
        if (idx==size){
          break;
        }
        pre_loc = loc;
        loc = bv->Value(idx);;
      }
      if (idx==size){
        break;
      }
      row_cnt+=cur_rows;
    }
  }

  if(!builder.Finish(&array).ok())
    throw std::runtime_error("Could not create result array.");
  return IntermediateResult{array};
}


IntermediateResult ScanParquetInt(std::shared_ptr<parquet::ParquetFileReader> parquet_reader, int col,
                                            std::shared_ptr<arrow::Array> pre = nullptr) {
  arrow::Int32Builder val_builder;
  std::shared_ptr<arrow::Array> val_array;
  std::shared_ptr<parquet::ColumnReader> column_reader;

  int n_rowgroup = parquet_reader->metadata()->num_row_groups();

  // this is for null check
  int16_t definition_level;
  int16_t repetition_level;

  int cnt = 0;

  if (pre == nullptr){
    // for each rou group, get col col chunk
    for (int i=0;i<n_rowgroup;i++){
      column_reader = parquet_reader->RowGroup(i)->Column(col);
      parquet::Int32Reader* int_reader =
              static_cast<parquet::Int32Reader *>(column_reader.get());

      int64_t  values_read = 0;

      while (int_reader->HasNext()) {
        int value;
        // Read one value at a time. The number of rows read is returned. values_read
        int_reader->ReadBatch(1, &definition_level, &repetition_level, &value, &values_read);
        val_builder.Append(value);
        cnt++;
      }
    }
  }
  else if (pre->length()>0){
    auto bv = std::static_pointer_cast<arrow::Int64Array>(pre);
    int idx = 0;
    int loc = bv->Value(idx);
    int row_cnt = 0;
    int pre_loc = 0;
    int value = 0.0;
    auto size = pre->length();
    for (int i=0;i<n_rowgroup;i++){
      int cur_rows = parquet_reader->RowGroup(i)->metadata()->num_rows();
      if (row_cnt + cur_rows<loc+1){
        row_cnt+=cur_rows;
        continue;
      }
      std::cout << "row group: "<< i<< std::endl;
      column_reader = parquet_reader->RowGroup(i)->Column(col);
      parquet::Int32Reader* int_reader =
              static_cast<parquet::Int32Reader*>(column_reader.get());
      int64_t  values_read = 0;
      pre_loc = row_cnt;
      // if didn't finish current row group.
      while (loc-row_cnt<cur_rows){
        int_reader->Skip(loc-pre_loc-1);
        int_reader->ReadBatch(1, &definition_level, &repetition_level, &value, &values_read);
        val_builder.Append(value);

        idx++;
        if (idx==size){
          break;
        }
        pre_loc = loc;
        loc = bv->Value(idx);;
      }
      if (idx==size){
        break;
      }
      row_cnt+=cur_rows;
    }
  }
  if(!val_builder.Finish(&val_array).ok())
    throw std::runtime_error("Could not create result array.");
  std::vector<std::shared_ptr<arrow::Array>> res;
  res.push_back(val_array);
  auto im = IntermediateResult{res};
  return im;
}


template<class T>
IntermediateResult FilterParquetString(std::shared_ptr<parquet::ParquetFileReader> parquet_reader, int col,
                                            std::string predicate, T func, std::shared_ptr<arrow::Array> pre = nullptr) {

  std::shared_ptr<parquet::ColumnReader> column_reader;
  std::shared_ptr<arrow::Array> array;
  parquet::ByteArray pred;
  pred.ptr = reinterpret_cast<const uint8_t*>(&predicate[0]);
  pred.len = predicate.length();


  int n_rowgroup = parquet_reader->metadata()->num_row_groups();

  // this is for null check
  int16_t definition_level;
  int16_t repetition_level;
  arrow::Int64Builder builder;

  int cnt = 0;

  if (pre == nullptr){
    // for each rou group, get col col chunk
    for (int i=0;i<n_rowgroup;i++){
      column_reader = parquet_reader->RowGroup(i)->Column(col);
      parquet::ByteArrayReader* byte_reader =
              static_cast<parquet::ByteArrayReader *>(column_reader.get());

      int64_t  values_read = 0;

      while (byte_reader->HasNext()) {
        parquet::ByteArray value;
        // Read one value at a time. The number of rows read is returned. values_read
         byte_reader->ReadBatch(1, &definition_level, &repetition_level, &value, &values_read);
        // There are no NULL values in the rows written
//        if (values_read == 0){
//          cnt++;
//          continue;
//        }
        if (func(value,pred)){
          builder.Append(cnt);
        }
        cnt++;
      }
    }
  }
  else if (pre->length()>0){
    auto bv = std::static_pointer_cast<arrow::Int64Array>(pre);
    int idx = 0;
    int loc = bv->Value(idx);
    int row_cnt = 0;
    int pre_loc = 0;
    parquet::ByteArray value;
    auto size = pre->length();
    for (int i=0;i<n_rowgroup;i++){
      int cur_rows = parquet_reader->RowGroup(i)->metadata()->num_rows();
      if (row_cnt + cur_rows<loc+1){
        row_cnt+=cur_rows;
        continue;
      }
      std::cout << "row group: "<< i<< std::endl;
      column_reader = parquet_reader->RowGroup(i)->Column(col);
      parquet::ByteArrayReader* byte_reader =
              static_cast<parquet::ByteArrayReader*>(column_reader.get());
      int64_t  values_read = 0;
      pre_loc = row_cnt;
      // if didn't finish current row group.
      while (loc-row_cnt<cur_rows){
        byte_reader->Skip(loc-pre_loc-1);
        byte_reader->ReadBatch(1, &definition_level, &repetition_level, &value, &values_read);
        if (func(value,pred)){
          builder.Append(loc);
        }
        idx++;
        if (idx==size){
          break;
        }
        pre_loc = loc;
        loc = bv->Value(idx);;
      }
      if (idx==size){
        break;
      }
      row_cnt+=cur_rows;
    }
  }

  if(!builder.Finish(&array).ok())
    throw std::runtime_error("Could not create result array.");
  return IntermediateResult{array};
}



IntermediateResult ScanParquetString(std::shared_ptr<parquet::ParquetFileReader> parquet_reader, int col,
                                     std::shared_ptr<arrow::Array> pre = nullptr) {
  arrow::StringBuilder val_builder;
  std::shared_ptr<arrow::Array> val_array;
  std::shared_ptr<parquet::ColumnReader> column_reader;

  int n_rowgroup = parquet_reader->metadata()->num_row_groups();

  // this is for null check
  int16_t definition_level;
  int16_t repetition_level;

  int cnt = 0;

  if (pre == nullptr){
    // for each rou group, get col col chunk
    for (int i=0;i<n_rowgroup;i++){
      column_reader = parquet_reader->RowGroup(i)->Column(col);
      parquet::ByteArrayReader* byte_reader =
              static_cast<parquet::ByteArrayReader *>(column_reader.get());

      int64_t  values_read = 0;

      while (byte_reader->HasNext()) {
        parquet::ByteArray value;
        // Read one value at a time. The number of rows read is returned. values_read
        byte_reader->ReadBatch(1, &definition_level, &repetition_level, &value, &values_read);
        // There are no NULL values in the rows written
        if (values_read == 0){
          cnt++;
          continue;
        }
        val_builder.Append(ByteArrayToString(value));

        cnt++;
      }
    }
  }
  else if (pre->length()>0){
    auto bv = std::static_pointer_cast<arrow::Int64Array>(pre);
    int idx = 0;
    int loc = bv->Value(idx);
    int row_cnt = 0;
    int pre_loc = 0;
    parquet::ByteArray value;
    auto size = pre->length();
    for (int i=0;i<n_rowgroup;i++){
      int cur_rows = parquet_reader->RowGroup(i)->metadata()->num_rows();
      if (row_cnt + cur_rows<loc+1){
        row_cnt+=cur_rows;
        continue;
      }
      std::cout << "row group: "<< i<< std::endl;
      column_reader = parquet_reader->RowGroup(i)->Column(col);
      parquet::ByteArrayReader* byte_reader =
              static_cast<parquet::ByteArrayReader*>(column_reader.get());
      int64_t  values_read = 0;
      pre_loc = row_cnt;
      // if didn't finish current row group.
      while (loc-row_cnt<cur_rows){
        byte_reader->Skip(loc-pre_loc-1);
        byte_reader->ReadBatch(1, &definition_level, &repetition_level, &value, &values_read);
        val_builder.Append(ByteArrayToString(value));

        idx++;
        if (idx==size){
          break;
        }
        pre_loc = loc;
        loc = bv->Value(idx);;
      }
      if (idx==size){
        break;
      }
      row_cnt+=cur_rows;
    }
  }
  if(!val_builder.Finish(&val_array).ok())
    throw std::runtime_error("Could not create result array.");
  std::vector<std::shared_ptr<arrow::Array>> res;
  res.push_back(val_array);
  auto im = IntermediateResult{res};
  return im;
}



IntermediateResult ScanParquetFile(std::string filename,  std::vector<int>* projs,  std::vector<int>* filters,
                                  std::vector<std::string>* ops, std::vector<std::string>* opands){


  auto begin = std::chrono::steady_clock::now();
  std::shared_ptr<parquet::ParquetFileReader> parquet_reader =
          parquet::ParquetFileReader::OpenFile(filename, false);
  auto end = std::chrono::steady_clock::now();
  auto t_load = std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count();
  auto cols = UnionVector(projs, filters);
  auto schema = parquet_reader->metadata()->schema();

  std::cout << "==Num of col: " << schema->num_columns()<<" and parquet loading time: "<< t_load<< std::endl;

  std::vector<std::shared_ptr<arrow::Array>> res;
  std::unordered_map<int, int> proj_map;
  for (auto i=0;i < (int)projs->size();i++){
    proj_map[projs->at(i)]=i;
    res.push_back(nullptr);
  }
  IntermediateResult im = IntermediateResult{res};
  std::cout << "Num of col: " << im.results().size()<< std::endl;
  // track qualified entries for filters and projections
  std::shared_ptr<arrow::Array> pre = nullptr;

  // start filtering first
  for (auto findex = 0; findex < (int)filters->size();findex++){
    IntermediateResult cur_im;
    auto col_idx = filters->at(findex);
    bool is_proj = IsProj(col_idx, projs);

    std::string predicate;
    auto attr = schema->Column(col_idx)->physical_type();
    switch (attr) {

      case parquet::Type::type::DOUBLE:
        predicate = opands->at(findex);
        if (ops->at(findex) == "EQUAL"){
          cur_im = FilterParquetDouble(parquet_reader,  col_idx,
                                       std::stod(predicate), doubleEqual, pre);
        }
        else if (ops->at(findex) == "GREATER"){
          cur_im = FilterParquetDouble(parquet_reader,  col_idx,
                                       std::stod(predicate), doubleGreater, pre);
        }
        else if (ops->at(findex) == "GREATER_EQUAL"){
          cur_im = FilterParquetDouble(parquet_reader,  col_idx,
                                       std::stod(predicate), doubleGE, pre);
        }
        else if (ops->at(findex) == "LESS"){
          cur_im = FilterParquetDouble(parquet_reader,  col_idx,
                                       std::stod(predicate), doubleLess, pre);
        }
        else if (ops->at(findex) == "LESS_EQUAL"){
          cur_im = FilterParquetDouble(parquet_reader,  col_idx,
                                       std::stod(predicate), doubleLE, pre);
        }
        break;
      case parquet::Type::type::INT32:
        predicate = opands->at(findex);
        if (ops->at(findex) == "EQUAL")
          cur_im = FilterParquetInt(parquet_reader,  col_idx, std::stoi(predicate),
                                      intEqual, pre);
        else if (ops->at(findex) == "GREATER")
          cur_im = FilterParquetInt(parquet_reader,  col_idx, std::stoi(predicate),
                                    intGreater, pre);
        else if (ops->at(findex) == "GREATER_EQUAL")
          cur_im = FilterParquetInt(parquet_reader,  col_idx, std::stoi(predicate),
                                    intGE, pre);
        else if (ops->at(findex) == "LESS_EQUAL")
          cur_im = FilterParquetInt(parquet_reader,  col_idx, std::stoi(predicate),
                                    intLE, pre);
        else if (ops->at(findex) == "LESS")
          cur_im = FilterParquetInt(parquet_reader,  col_idx, std::stoi(predicate),
                                    intLess, pre);
        break;
      case parquet::Type::type::BYTE_ARRAY:
        predicate = opands->at(findex);
        if (ops->at(findex) == "EQUAL")
          cur_im = FilterParquetString(parquet_reader,  col_idx, predicate,
                                       ByteArrayEqual, pre);
        else
          throw std::runtime_error(ops->at(findex) + " operator is not supported for string yet.");
        break;
      default:
        throw std::runtime_error(TypeToString(attr) + " type is not supported yet.");

    }
    pre = cur_im.results().at(0);
    std::cout << "finished filtering on "<<col_idx<< " with atrr "<<TypeToString(attr) <<": " << pre->length() << std::endl;
  }

  std::cout << "finished filtering: " << std::endl;

  // then handle projection col
  for (auto pindex = 0; pindex < (int)projs->size();pindex++){
    IntermediateResult cur_im;
    auto col_idx = projs->at(pindex);
    auto attr = schema->Column(col_idx)->physical_type();
    IntermediateResult cur_proj;

    switch (attr) {
      case parquet::Type::type::DOUBLE:
        cur_proj = ScanParquetDouble(parquet_reader, col_idx,pre);
        im.results().at(proj_map[col_idx]) = cur_proj.results().at(0);
        break;
      case parquet::Type::type::INT32:
        cur_proj = ScanParquetInt(parquet_reader, col_idx,pre);
        im.results().at(proj_map[col_idx]) = cur_proj.results().at(0);
        break;
      case parquet::Type::type::BYTE_ARRAY:
        cur_proj = ScanParquetString(parquet_reader, col_idx,pre);
        im.results().at(proj_map[col_idx]) = cur_proj.results().at(0);
        break;
      default:
        throw std::runtime_error(TypeToString(attr) + " type is not supported yet.");
    }


  }

  return im;
}


