//
// Created by Chunwei Liu on 8/25/21.
//

#ifndef ARROW_ARROW_HELPER_H
#define ARROW_ARROW_HELPER_H

#endif //ARROW_ARROW_HELPER_H
#include <unordered_map>
#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/array.h>
#include <arrow/table.h>
#include <arrow/type_fwd.h>
#include <arrow/chunked_array.h>
#include <vector>
#include <set>
#include <iostream>
#include "v_util.h"

class IntermediateResult {
public:
    explicit IntermediateResult(const std::shared_ptr<arrow::Array>& result)
    : IntermediateResult(std::vector<std::shared_ptr<arrow::Array>>{result})
    { }

    explicit IntermediateResult(std::vector<std::shared_ptr<arrow::Array>> results)
    : results_(std::move(results))
    { }

    void AppendResult(const std::shared_ptr<arrow::Array>& result){
      results_.push_back(result);
    }

    IntermediateResult() = default;
    IntermediateResult(const IntermediateResult&) = default;
    IntermediateResult(IntermediateResult&&) = default;
    IntermediateResult& operator=(const IntermediateResult&) = default;

    [[nodiscard]]  std::vector<std::shared_ptr<arrow::Array>>& results() { return results_; }

private:
    std::vector<std::shared_ptr<arrow::Array>> results_;
};

inline bool doubleLess(double a, double b){
  return a<b;
}

inline bool doubleLE(double a, double b){
  return a<+b;
}

inline bool doubleEqual(double a, double b){
  return a==b;
  //  return fabs(a - b) < EPSILON;
}

inline bool doubleGreater(double a, double b){
  return a>b;
}

inline bool doubleGE(double a, double b){
  return a>=b;
}

inline bool intLess(int a, int b){
  return a<b;
}

inline bool intLE(int a, int b){
  return a<+b;
}

inline bool intEqual(int a, int b){
  return a==b;
}

inline bool intGreater(int a, int b){
  return a>b;
}

inline bool intGE(int a, int b){
  return a>=b;
}

inline bool strEqual(std::string a, std::string b){
  return a==b;
}

int64_t DictTranslate(std::shared_ptr<arrow::StringArray> dictionary, std::string opand){
  auto size = dictionary->length();
  for (int64_t i = 0; i<size; i++){
    if (opand==dictionary->GetString(i)){
      return i;
    }
  }

}

std::vector<int>* MatchVector(std::vector<int>* projs,  std::vector<int>* filters){
  std::vector<int> *inter = nullptr;
  for (int p: *projs){
    for (int f: *filters){
      if (p==f){
        inter->push_back(p);
        break;
      }
    }
  }
  return inter;
}

std::vector<int> UnionVector(std::vector<int>* projs,  std::vector<int>* filters){
  std::vector<int> un;
  std::set<int> set;
  for (int p: *projs){
    set.insert(p);
  }
  for (int f: *filters){
    set.insert(f);
  }
  std::set<int>::iterator it = set.begin();

  while (it != set.end()){
    un.push_back(*it);
    it++;
  }
  return un;
}

bool IsProj (int x, std::vector<int>* projs){
  for (int p : *projs){
    if (p==x)
      return true;
  }
  return false;
}



/// filter a double data chunk with given condition, output is list of qualified row number
/// \tparam T
/// \param chunk
/// \param idx_builder
/// \param pred: opand
/// \param func: predicate evaluation function
/// \param projected
/// \param pre
/// \param base
template<class T>
        void FilterDoubleChunk(std::shared_ptr<arrow::Array>  chunk, arrow::Int64Builder& idx_builder, double pred,
                             T func, bool projected = false, std::vector<int64_t>* pre = nullptr, int64_t base = 0){

  auto doublechunk=std::static_pointer_cast<arrow::DoubleArray>(chunk);
  double val = 0.0;
  if (pre != nullptr && pre->size()==0){
    return ;
  }
  else if (pre == nullptr){
    for (int64_t idx = 0; idx < chunk->length(); idx++){
      val = doublechunk->Value(idx);
      if (func(val,pred)){
        idx_builder.Append(idx+base);
      }
    }
  }
  else{
    int len = pre->size();
    int64_t loc = 0;
    for (int64_t idx = 0; idx < len; idx++){
      loc =pre->at(idx);
      val = doublechunk->Value(loc-base);
      if (func(val,pred)){
        idx_builder.Append(loc);
      }
    }
  }
}

/// filter a int data chunk with given condition, output is list of qualified row number
/// \tparam T
/// \param chunk
/// \param idx_builder
/// \param pred
/// \param func
/// \param projected
/// \param pre
/// \param base
template<class T>
        void FilterInt32Chunk(std::shared_ptr<arrow::Array>  chunk, arrow::Int64Builder& idx_builder, int pred,
                             T func, bool projected = false, std::vector<int64_t>* pre = nullptr, int64_t base = 0){

  auto intchunk=std::static_pointer_cast<arrow::Int32Array>(chunk);
  int val = 0;
  if (pre != nullptr && pre->size()==0){
    return ;
  }
  else if (pre == nullptr){
    for (int64_t idx = 0; idx < chunk->length(); idx++){
      val = intchunk->Value(idx);
      if (func(val,pred)){
        idx_builder.Append(idx+base);

      }
    }
  }
  else{
    int len = pre->size();
    int64_t loc = 0;
    for (int64_t idx = 0; idx < len; idx++){
      loc =pre->at(idx);
      val =intchunk->Value(loc-base);
      if (func(val,pred)){
        idx_builder.Append(loc);
      }
    }
  }
}

/// filter a string data chunk with given condition, output is list of qualified row number
/// \tparam T
/// \param chunk
/// \param idx_builder
/// \param pred
/// \param func
/// \param projected
/// \param pre
/// \param base
template<class T>
        void FilterStringChunk(std::shared_ptr<arrow::Array>  chunk, arrow::Int64Builder& idx_builder, std::string pred,
                              T func, bool projected = false, std::vector<int64_t>* pre = nullptr, int64_t base = 0){

  auto intchunk=std::static_pointer_cast<arrow::StringArray>(chunk);
  std::string val = "";
  if (pre != nullptr && pre->size()==0){
    return ;
  }
  else if (pre == nullptr){
    for (int64_t idx = 0; idx < chunk->length(); idx++){
      val = intchunk->GetString(idx);
      if (func(val,pred)){
        idx_builder.Append(idx+base);

      }
    }
  }
  else{
    int len = pre->size();
    int64_t loc = 0;
    for (int64_t idx = 0; idx < len; idx++){
      loc =pre->at(idx);
      val =intchunk->GetString(loc-base);
      if (func(val,pred)){
        idx_builder.Append(loc);
      }
    }
  }
}


/// filter a dictionary (string) data chunk with given condition, output is list of qualified row number
/// \tparam T
/// \param chunk
/// \param idx_builder
/// \param pred
/// \param func
/// \param projected
/// \param pre
/// \param base
template<class T>
        void FilterDictStringChunk(std::shared_ptr<arrow::Array>  chunk, arrow::Int64Builder& idx_builder, std::string predicate,
                               T func, bool projected = false, std::vector<int64_t>* pre = nullptr, int64_t base = 0){

  auto intchunk=std::static_pointer_cast<arrow::DictionaryArray>(chunk);
  auto dict = std::static_pointer_cast<arrow::StringArray>(intchunk->dictionary());

  // query rewriting by dictionary translation
  auto pred = DictTranslate(dict, predicate);
  int64_t val = 0;
  if (pre != nullptr && pre->size()==0){
    return ;
  }
  else if (pre == nullptr){
    for (int64_t idx = 0; idx < chunk->length(); idx++){
      val = intchunk->GetValueIndex(idx);
      if (func(val,pred)){
        idx_builder.Append(idx+base);

      }
    }
  }
  else{
    int len = pre->size();
    int64_t loc = 0;
    for (int64_t idx = 0; idx < len; idx++){
      loc =pre->at(idx);
      val =intchunk->GetValueIndex(loc-base);
      if (func(val,pred)){
        idx_builder.Append(loc);
      }
    }
  }
}


/// Scan double data chunk based on the pre results
/// \param chunk
/// \param val_builder
/// \param pre
/// \param base
void ScanDataChunk(std::shared_ptr<arrow::Array> chunk, arrow::DoubleBuilder& val_builder,
                   std::vector<int64_t>* pre = nullptr, int base = 0){

  auto doublechunk=std::static_pointer_cast<arrow::DoubleArray>(chunk);
  double val = 0.0;
  if (pre != nullptr && pre->size()==0){
    return ;
  }
  else if (pre == nullptr){
    for (int64_t idx = 0; idx < chunk->length(); idx++){
      val = doublechunk->Value(idx);
      val_builder.Append(val);
    }
  }
  else{
    int len = pre->size();
    int64_t loc = 0;
    for (int64_t idx = 0; idx < len; idx++){
      loc =pre->at(idx);
      val = doublechunk->Value(loc-base);
      val_builder.Append(val);
    }
  }
}

/// Scan int data chunk based on the pre results
/// \param chunk
/// \param val_builder
/// \param pre
/// \param base
void ScanDataChunk(std::shared_ptr<arrow::Array> chunk, arrow::Int32Builder& val_builder,
                   std::vector<int64_t>* pre = nullptr, int base = 0){

  auto intchunk=std::static_pointer_cast<arrow::Int32Array>(chunk);
  double val = 0.0;
  if (pre != nullptr && pre->size()==0){
    return ;
  }
  else if (pre == nullptr){
    for (int64_t idx = 0; idx < chunk->length(); idx++){
      val = intchunk->Value(idx);
      val_builder.Append(val);
    }
  }
  else{
    int len = pre->size();
    int64_t loc = 0;
    for (int64_t idx = 0; idx < len; idx++){
      loc = pre->at(idx);
      val = intchunk->Value(loc-base);
      val_builder.Append(val);
    }
  }
}


/// Scan string data chunk based on the pre results
/// \param chunk
/// \param val_builder
/// \param pre
/// \param base
void ScanDataChunk(std::shared_ptr<arrow::Array> chunk, arrow::StringBuilder& val_builder,
                   std::vector<int64_t>* pre = nullptr, int base = 0){

  auto stringchunk=std::static_pointer_cast<arrow::StringArray>(chunk);
  std::string val = "";
  if (pre != nullptr && pre->size()==0){
    return ;
  }
  else if (pre == nullptr){
    for (int64_t idx = 0; idx < chunk->length(); idx++){
      val = stringchunk->GetString(idx);
      val_builder.Append(val);
    }
  }
  else{
    int len = pre->size();
    int64_t loc = 0;
    for (int64_t idx = 0; idx < len; idx++){
      loc = pre->at(idx);
      val = stringchunk->GetString(loc-base);
      val_builder.Append(val);
    }
  }
}


/// Scan dictionary (string) data chunk based on the pre results
/// \param chunk
/// \param val_builder
/// \param pre
/// \param base
void ScanDataChunkDict(std::shared_ptr<arrow::Array> chunk, arrow::StringBuilder& val_builder,
                   std::vector<int64_t>* pre = nullptr, int base = 0){

  auto stringchunk=std::static_pointer_cast<arrow::DictionaryArray>(chunk);
  std::string val = "";
  if (pre != nullptr && pre->size()==0){
    return ;
  }
  else if (pre == nullptr){
    auto dict = std::static_pointer_cast<arrow::StringArray>(stringchunk->dictionary());
    for (int64_t idx = 0; idx < chunk->length(); idx++){
      val = dict->GetString(stringchunk->GetValueIndex(idx));;
      val_builder.Append(val);
    }
  }
  else{
    int len = pre->size();
    auto dict = std::static_pointer_cast<arrow::StringArray>(stringchunk->dictionary());
    int64_t loc = 0;
    for (int64_t idx = 0; idx < len; idx++){
      loc = pre->at(idx);
      val = dict->GetString(stringchunk->GetValueIndex(loc-base));
      val_builder.Append(val);
    }
  }
}


/// filter a whole chunked array with double type
/// \tparam T
/// \param c_array
/// \param pred
/// \param func
/// \param projected
/// \param pre
/// \return
template<class T>
        IntermediateResult FilterChunkedArray(std::shared_ptr<arrow::ChunkedArray> c_array, double pred, T func,
                                             bool projected = false, std::shared_ptr<arrow::Array> pre = nullptr){
  arrow::Int64Builder idx_builder;
  std::shared_ptr<arrow::Array> idx_array;

  auto n_chunks = c_array->num_chunks();
  auto base = 0;
  auto cur_num = 0;


  if (pre == nullptr){
    for (auto i=0;i<n_chunks;i++){
      auto cur_array = c_array->chunk(i);
      cur_num = cur_array->length();
      FilterDoubleChunk(cur_array,idx_builder, pred, func, projected, nullptr, base);
      base+=cur_num;
    }
    if(!idx_builder.Finish(&idx_array).ok())
      throw std::runtime_error("Could not create result array.");
    std::vector<std::shared_ptr<arrow::Array>> res;
    res.push_back(idx_array);
    auto im = IntermediateResult{res};
    return im;
  }


  auto bv = pre->data();
  auto mark = 0;

  for (auto i=0;i<n_chunks;i++){
    auto cur_array = c_array->chunk(i);
    cur_num = cur_array->length();
    std::vector<int64_t> local_bv;

    while (mark<pre->length() && (int64_t)bv->GetValues<int64_t>(mark) < base+cur_num){
      local_bv.push_back((int64_t)bv->GetValues<int64_t>(mark));
      mark++;
    }

    FilterDoubleChunk(cur_array,idx_builder, pred, func, projected, &local_bv, base);

    base+=cur_num;
  }
  if(!idx_builder.Finish(&idx_array).ok())
    throw std::runtime_error("Could not create idx array.");
  std::vector<std::shared_ptr<arrow::Array>> res;
  res.push_back(idx_array);
  auto im = IntermediateResult{res};
  return im;
}



/// filter a whole chunked array with int type
/// \tparam T
/// \param c_array
/// \param pred
/// \param func
/// \param projected
/// \param pre
/// \return
template<class T>
        IntermediateResult FilterChunkedArray(std::shared_ptr<arrow::ChunkedArray> c_array, int pred, T func,
                                             bool projected = false, std::shared_ptr<arrow::Array> pre = nullptr){
  arrow::Int64Builder idx_builder;
  std::shared_ptr<arrow::Array> idx_array;

  auto n_chunks = c_array->num_chunks();
  auto base = 0;
  auto cur_num = 0;

  if (pre == nullptr){
    for (auto i=0;i<n_chunks;i++){
      auto cur_array = c_array->chunk(i);
      cur_num = cur_array->length();
      FilterInt32Chunk(cur_array,idx_builder, pred, func, projected, nullptr, base);
      base+=cur_num;
    }
    if(!idx_builder.Finish(&idx_array).ok())
      throw std::runtime_error("Could not create result array.");
    std::vector<std::shared_ptr<arrow::Array>> res;
    res.push_back(idx_array);
    auto im = IntermediateResult{res};
    return im;
  }


  auto bv = std::static_pointer_cast<arrow::Int64Array>(pre);
  auto mark = 0;

  for (auto i=0;i<n_chunks;i++){
    auto cur_array = c_array->chunk(i);
    cur_num = cur_array->length();
    std::vector<int64_t> local_bv;

    while (mark<pre->length() && bv->Value(mark) < base+cur_num){
      local_bv.push_back(bv->Value(mark));
      mark++;
    }

    FilterInt32Chunk(cur_array,idx_builder, pred, func, projected, &local_bv, base);
    base+=cur_num;
  }
  if(!idx_builder.Finish(&idx_array).ok())
    throw std::runtime_error("Could not create idx array.");
  std::vector<std::shared_ptr<arrow::Array>> res;
  res.push_back(idx_array);
  auto im = IntermediateResult{res};
  return im;
}


/// filter a whole chunked array with string type
/// \tparam T
/// \param c_array
/// \param pred
/// \param func
/// \param projected
/// \param pre
/// \return
template<class T>
        IntermediateResult FilterChunkedArray(std::shared_ptr<arrow::ChunkedArray> c_array, std::string pred, T func,
                                              bool projected = false, std::shared_ptr<arrow::Array> pre = nullptr){
  arrow::Int64Builder idx_builder;
  std::shared_ptr<arrow::Array> idx_array;

  auto n_chunks = c_array->num_chunks();
  auto base = 0;
  auto cur_num = 0;

  if (pre == nullptr){
    for (auto i=0;i<n_chunks;i++){
      auto cur_array = c_array->chunk(i);
      cur_num = cur_array->length();
      FilterStringChunk(cur_array,idx_builder, pred, func, projected, nullptr, base);
      base+=cur_num;
    }
    if(!idx_builder.Finish(&idx_array).ok())
      throw std::runtime_error("Could not create result array.");
    std::vector<std::shared_ptr<arrow::Array>> res;
    res.push_back(idx_array);
    auto im = IntermediateResult{res};
    return im;
  }


  auto bv = std::static_pointer_cast<arrow::Int64Array>(pre);
  auto mark = 0;

  for (auto i=0;i<n_chunks;i++){
    auto cur_array = c_array->chunk(i);
    cur_num = cur_array->length();
    std::vector<int64_t> local_bv;

    while (mark<pre->length() && bv->Value(mark) < base+cur_num){
      local_bv.push_back(bv->Value(mark));
      mark++;
    }

    FilterStringChunk(cur_array,idx_builder, pred, func, projected, &local_bv, base);
    base+=cur_num;
  }
  if(!idx_builder.Finish(&idx_array).ok())
    throw std::runtime_error("Could not create idx array.");
  std::vector<std::shared_ptr<arrow::Array>> res;
  res.push_back(idx_array);
  auto im = IntermediateResult{res};
  return im;
}

/// filter a whole chunked array with dictionary (string) type
/// \tparam T
/// \param c_array
/// \param pred
/// \param func
/// \param projected
/// \param pre
/// \return
template<class T>
        IntermediateResult FilterDictChunkedArray(std::shared_ptr<arrow::ChunkedArray> c_array, std::string pred, T func,
                                              bool projected = false, std::shared_ptr<arrow::Array> pre = nullptr){
  arrow::Int64Builder idx_builder;
  std::shared_ptr<arrow::Array> idx_array;

  auto n_chunks = c_array->num_chunks();
  auto base = 0;
  auto cur_num = 0;

  if (pre == nullptr){
    for (auto i=0;i<n_chunks;i++){
      auto cur_array = c_array->chunk(i);
      cur_num = cur_array->length();
      FilterDictStringChunk(cur_array,idx_builder, pred, func, projected, nullptr, base);
      base+=cur_num;
    }
    if(!idx_builder.Finish(&idx_array).ok())
      throw std::runtime_error("Could not create result array.");
    std::vector<std::shared_ptr<arrow::Array>> res;
    res.push_back(idx_array);
    auto im = IntermediateResult{res};
    return im;
  }


  auto bv = std::static_pointer_cast<arrow::Int64Array>(pre);
  auto mark = 0;

  for (auto i=0;i<n_chunks;i++){
    auto cur_array = c_array->chunk(i);
    cur_num = cur_array->length();
    std::vector<int64_t> local_bv;

    while (mark<pre->length() && bv->Value(mark) < base+cur_num){
      local_bv.push_back(bv->Value(mark));
      mark++;
    }

    FilterDictStringChunk(cur_array,idx_builder, pred, func, projected, &local_bv, base);
    base+=cur_num;
  }
  if(!idx_builder.Finish(&idx_array).ok())
    throw std::runtime_error("Could not create idx array.");
  std::vector<std::shared_ptr<arrow::Array>> res;
  res.push_back(idx_array);
  auto im = IntermediateResult{res};
  return im;
}

/// scan chunked array with double type
///
/// \param c_array
/// \param pre
/// \return
IntermediateResult ScanArrowDouble(std::shared_ptr<arrow::ChunkedArray> c_array,  std::shared_ptr<arrow::Array> pre = nullptr){

  arrow::DoubleBuilder val_builder;
  std::shared_ptr<arrow::Array> val_array;

  auto n_chunks = c_array->num_chunks();
  auto base = 0;
  auto cur_num = 0;
  if (pre == nullptr){
    for (auto i=0;i<n_chunks;i++){
      auto cur_array = c_array->chunk(i);
      ScanDataChunk(cur_array, val_builder, nullptr, base);
      base+=cur_num;
    }
    if(!val_builder.Finish(&val_array).ok())
      throw std::runtime_error("Could not create result array.");
    std::vector<std::shared_ptr<arrow::Array>> res;
    res.push_back(val_array);
    auto im = IntermediateResult{res};
    return im;
  }

  auto mark = 0;
  auto bv = pre->data();
  for (auto i=0;i<n_chunks;i++){
    auto cur_array = c_array->chunk(i);
    cur_num = cur_array->length();
    std::vector<int64_t> local_bv;

    while (mark<pre->length() && (int64_t)bv->GetValues<int64_t>(mark) < base+cur_num){
      local_bv.push_back((int64_t)bv->GetValues<int64_t>(mark));
      mark++;
    }

    ScanDataChunk(cur_array, val_builder, &local_bv, base);

    base+=cur_num;
  }

  if(!val_builder.Finish(&val_array).ok())
    throw std::runtime_error("Could not create result array.");
  std::vector<std::shared_ptr<arrow::Array>> res;
  res.push_back(val_array);
  auto im = IntermediateResult{res};
  return im;
}


/// scan chunked array with int type
///
/// \param c_array
/// \param pre
/// \return
IntermediateResult ScanArrowInt32(std::shared_ptr<arrow::ChunkedArray> c_array,  std::shared_ptr<arrow::Array> pre = nullptr){

  arrow::Int32Builder val_builder;
  std::shared_ptr<arrow::Array> val_array;

  auto n_chunks = c_array->num_chunks();
  auto base = 0;
  auto cur_num = 0;


  if (pre == nullptr){
    for (auto i=0;i<n_chunks;i++){
      auto cur_array = c_array->chunk(i);
      cur_num = cur_array->length();
      ScanDataChunk(cur_array, val_builder, nullptr, base);
      base+=cur_num;
    }
    if(!val_builder.Finish(&val_array).ok())
      throw std::runtime_error("Could not create result array.");
    std::vector<std::shared_ptr<arrow::Array>> res;
    res.push_back(val_array);
    auto im = IntermediateResult{res};
    return im;
  }

  auto mark = 0;
  auto bv = std::static_pointer_cast<arrow::Int64Array>(pre);

  for (auto i=0;i<n_chunks;i++){
    auto cur_array = c_array->chunk(i);
    cur_num = cur_array->length();
    std::vector<int64_t> local_bv;

    while (mark<bv->length() && bv->Value(mark) < base+cur_num){
      local_bv.push_back(bv->Value(mark));
      mark++;
    }


    ScanDataChunk(cur_array, val_builder, &local_bv, base);

    base+=cur_num;
  }

  if(!val_builder.Finish(&val_array).ok())
    throw std::runtime_error("Could not create result array.");
  std::vector<std::shared_ptr<arrow::Array>> res;
  res.push_back(val_array);
  auto im = IntermediateResult{res};
  return im;
}


/// scan chunked array with string type
///
/// \param c_array
/// \param pre
/// \return
IntermediateResult ScanArrowString(std::shared_ptr<arrow::ChunkedArray> c_array,  std::shared_ptr<arrow::Array> pre = nullptr){

  arrow::StringBuilder val_builder;
  std::shared_ptr<arrow::Array> val_array;

  auto n_chunks = c_array->num_chunks();
  auto base = 0;
  auto cur_num = 0;

  if (pre == nullptr){
    for (auto i=0;i<n_chunks;i++){
      auto cur_array = c_array->chunk(i);
      cur_num = cur_array->length();
      ScanDataChunk(cur_array, val_builder, nullptr, base);
      base+=cur_num;
    }
    if(!val_builder.Finish(&val_array).ok())
      throw std::runtime_error("Could not create result array.");
    std::vector<std::shared_ptr<arrow::Array>> res;
    res.push_back(val_array);
    auto im = IntermediateResult{res};
    return im;
  }

  auto mark = 0;
  auto bv = std::static_pointer_cast<arrow::Int64Array>(pre);

  for (auto i=0;i<n_chunks;i++){
    auto cur_array = c_array->chunk(i);
    cur_num = cur_array->length();
    std::vector<int64_t> local_bv;

    while (mark<bv->length() && bv->Value(mark) < base+cur_num){
      local_bv.push_back(bv->Value(mark));
      mark++;
    }


    ScanDataChunk(cur_array, val_builder, &local_bv, base);

    base+=cur_num;
  }

  if(!val_builder.Finish(&val_array).ok())
    throw std::runtime_error("Could not create result array.");
  std::vector<std::shared_ptr<arrow::Array>> res;
  res.push_back(val_array);
  auto im = IntermediateResult{res};
  return im;
}


/// scan chunked array with dictinary (string) type
///
/// \param c_array
/// \param pre
/// \return
IntermediateResult ScanArrowDict(std::shared_ptr<arrow::ChunkedArray> c_array,  std::shared_ptr<arrow::Array> pre = nullptr){

  arrow::StringBuilder val_builder;
  std::shared_ptr<arrow::Array> val_array;

  auto n_chunks = c_array->num_chunks();
  auto base = 0;
  auto cur_num = 0;

  if (pre == nullptr){
    for (auto i=0;i<n_chunks;i++){
      auto cur_array = c_array->chunk(i);
      cur_num = cur_array->length();
      ScanDataChunkDict(cur_array, val_builder, nullptr, base);
      base+=cur_num;
    }
    if(!val_builder.Finish(&val_array).ok())
      throw std::runtime_error("Could not create result array.");
    std::vector<std::shared_ptr<arrow::Array>> res;
    res.push_back(val_array);
    auto im = IntermediateResult{res};
    return im;
  }

  auto mark = 0;
  auto bv = std::static_pointer_cast<arrow::Int64Array>(pre);

  for (auto i=0;i<n_chunks;i++){
    auto cur_array = c_array->chunk(i);
    cur_num = cur_array->length();
    std::vector<int64_t> local_bv;

    while (mark<bv->length() && bv->Value(mark) < base+cur_num){
      local_bv.push_back(bv->Value(mark));
      mark++;
    }

    ScanDataChunkDict(cur_array, val_builder, &local_bv, base);

    base+=cur_num;
  }

  if(!val_builder.Finish(&val_array).ok())
    throw std::runtime_error("Could not create result array.");
  std::vector<std::shared_ptr<arrow::Array>> res;
  res.push_back(val_array);
  auto im = IntermediateResult{res};
  return im;
}




IntermediateResult ScanArrowTable(std::string filename,  std::vector<int>* projs,  std::vector<int>* filters,
                                  std::vector<std::string>* ops, std::vector<std::string>* opands){
  std::shared_ptr<arrow::Table> table;
  auto cols = UnionVector(projs, filters);

  std::unordered_map<int, int> col_map;
  int count = 0;
  for (int col: cols){
    col_map[col]=count;
    std::cout <<col<< "---" << count<< std::endl;
    count++;
  }

  read_feather_column_to_table(filename, &table, cols);
  auto schema = table->schema();

  std::cout << "Num of col: " << schema->num_fields()<< std::endl;
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

    std::cout << "is projected: " << is_proj << std::endl;

    std::string predicate;
    auto attr = schema->field(col_map[col_idx])->type();
     switch (attr->id()) {

       case arrow::Type::DOUBLE:
         predicate = opands->at(findex);
         if (ops->at(findex) == "EQUAL"){
           cur_im = FilterChunkedArray(table->column(col_map[col_idx]), std::stod(predicate),
                                                                     doubleEqual, is_proj,  pre);
         }
         else if (ops->at(findex) == "GREATER"){
           cur_im = FilterChunkedArray(table->column(col_map[col_idx]), std::stod(predicate),
                                       doubleGreater, is_proj,  pre);
         }
         else if (ops->at(findex) == "GREATER_EQUAL"){
           cur_im = FilterChunkedArray(table->column(col_map[col_idx]), std::stod(predicate),
                                       doubleGE, is_proj,  pre);
         }
         else if (ops->at(findex) == "LESS"){
           cur_im = FilterChunkedArray(table->column(col_map[col_idx]), std::stod(predicate),
                                       doubleLess, is_proj,  pre);
         }
         else if (ops->at(findex) == "LESS_EQUAL"){
           cur_im = FilterChunkedArray(table->column(col_map[col_idx]), std::stod(predicate),
                                       doubleLE, is_proj,  pre);
         }
         break;
       case arrow::Type::INT32:
         predicate = opands->at(findex);
         if (ops->at(findex) == "EQUAL")
           cur_im = FilterChunkedArray(table->column(col_map[col_idx]),  std::stoi(predicate),
                                       intEqual, is_proj,  pre);
         else if (ops->at(findex) == "GREATER")
           cur_im = FilterChunkedArray(table->column(col_map[col_idx]),  std::stoi(predicate),
                                       intGreater, is_proj,  pre);
         else if (ops->at(findex) == "GREATER_EQUAL")
           cur_im = FilterChunkedArray(table->column(col_map[col_idx]),  std::stoi(predicate),
                                       intGE, is_proj,  pre);
         else if (ops->at(findex) == "LESS_EQUAL")
           cur_im = FilterChunkedArray(table->column(col_map[col_idx]),  std::stoi(predicate),
                                       intLE, is_proj,  pre);
         else if (ops->at(findex) == "LESS")
           cur_im = FilterChunkedArray(table->column(col_map[col_idx]),  std::stoi(predicate),
                                       intLess, is_proj,  pre);
         break;
       case arrow::Type::STRING:
         predicate = opands->at(findex);
         if (ops->at(findex) == "EQUAL")
           cur_im = FilterChunkedArray(table->column(col_map[col_idx]),  predicate,
                                       strEqual, is_proj,  pre);
         else
           throw std::runtime_error(ops->at(findex) + " operator is not supported for string yet.");
         break;
       case arrow::Type::DICTIONARY:
         predicate = opands->at(findex);
         if (ops->at(findex) == "EQUAL")
           cur_im = FilterDictChunkedArray(table->column(col_map[col_idx]),  predicate,
                                       intEqual, is_proj,  pre);
         else
           throw std::runtime_error(ops->at(findex) + " operator is not supported for string yet.");
         break;
       default:
         throw std::runtime_error(attr->name() + " type is not supported yet.");

    }
    pre = cur_im.results().at(0);
     std::cout << "finished filtering on "<<col_idx<< " with atrr "<<attr->name()<<": " << pre->length() << std::endl;
  }

  std::cout << "finished filtering: " << pre->length() << std::endl;

  // then handle projection col
  for (auto pindex = 0; pindex < (int)projs->size();pindex++){
    IntermediateResult cur_im;
    auto col_idx = projs->at(pindex);
    auto attr = schema->field(col_map[col_idx])->type();
    std::cout << "pre length: " << pre->length() << std::endl;
    std::cout << "index of map: " << col_map[col_idx] << std::endl;
    switch (attr->id()) {
      case arrow::Type::DOUBLE:
        if (pre==nullptr){
          IntermediateResult cur_proj = ScanArrowDouble(table->column(col_map[col_idx]), pre);
          im.results().at(proj_map[col_idx]) = cur_proj.results().at(0);
        }
        else if (pre->length()==0){
          std::shared_ptr<arrow::Array> val_array;
          im.results().at(proj_map[col_idx]) = val_array;
        }
        else {
          IntermediateResult cur_proj = ScanArrowDouble(table->column(col_map[col_idx]), pre);
          im.results().at(proj_map[col_idx]) = cur_proj.results().at(0);
        }
        break;
      case arrow::Type::INT32:
        if (pre==nullptr){
          IntermediateResult cur_proj = ScanArrowInt32(table->column(col_map[col_idx]), pre);
          im.results().at(proj_map[col_idx]) = cur_proj.results().at(0);
        }
        else if (pre->length()==0){
          std::shared_ptr<arrow::Array> val_array;
          im.results().at(proj_map[col_idx]) = val_array;
        }
        else {
          IntermediateResult cur_proj = ScanArrowInt32(table->column(col_map[col_idx]), pre);
          std::cout << "col idx: " << col_idx<< " idx in result set "<< col_map[col_idx] << std::endl;
          im.results().at(proj_map[col_idx]) = cur_proj.results().at(0);
        }
        break;
      case arrow::Type::STRING:
        if (pre==nullptr){
          IntermediateResult cur_proj = ScanArrowString(table->column(col_map[col_idx]), pre);
          im.results().at(proj_map[col_idx]) = cur_proj.results().at(0);
        }
        else if (pre->length()==0){
          std::shared_ptr<arrow::Array> val_array;
          im.results().at(proj_map[col_idx]) = val_array;
        }
        else {
          IntermediateResult cur_proj = ScanArrowString(table->column(col_map[col_idx]), pre);
          std::cout << "col idx: " << col_idx<< " idx in result set "<< col_map[col_idx] << std::endl;
          im.results().at(proj_map[col_idx]) = cur_proj.results().at(0);
        }
        break;

      case arrow::Type::DICTIONARY:
        if (pre==nullptr){
          IntermediateResult cur_proj = ScanArrowDict(table->column(col_map[col_idx]), pre);
          im.results().at(proj_map[col_idx]) = cur_proj.results().at(0);
        }
        else if (pre->length()==0){
          std::shared_ptr<arrow::Array> val_array;
          im.results().at(proj_map[col_idx]) = val_array;
        }
        else {
          IntermediateResult cur_proj = ScanArrowDict(table->column(col_map[col_idx]), pre);
          std::cout << "col idx: " << col_idx<< " idx in result set "<< col_map[col_idx] << std::endl;
          im.results().at(proj_map[col_idx]) = cur_proj.results().at(0);
        }
        break;
      default:
        throw std::runtime_error(attr->name() + " type is not supported yet.");
    }

  }
  return im;
}



template<class T>
        void FilterDataChunkOnePass(arrow::DoubleArray* chunk, arrow::Int64Builder& idx_builder,
                                    arrow::DoubleBuilder& val_builder, double pred, T func, bool projected = false,
                                    std::vector<int>* pre = nullptr, int base = 0){


  double val = 0.0;
  if (pre != nullptr && pre->size()==0){
    return ;
  }
  else if (pre == nullptr){
    if (projected){
      for (int64_t idx = 0; idx < chunk->length(); idx++){
        val = chunk->Value(idx);
        if (func(val,pred)){
          idx_builder.Append(idx+base);
          val_builder.Append(val);
        }
      }
    }
    else {
      for (int64_t idx = 0; idx < chunk->length(); idx++){
        val = chunk->Value(idx);
        if (func(val,pred)){
          idx_builder.Append(idx+base);
        }
      }
    }
  }
  else{
    int len = pre->size();
    int64_t loc = 0;
    if (projected){
      for (int64_t idx = 0; idx < len; idx++){
        loc =pre->at(idx);
        val = chunk->Value(loc-base);
        if (func(val,pred)){
          idx_builder.Append(loc);
          val_builder.Append(val);
        }
      }

    }
    else {
      for (int64_t idx = 0; idx < chunk->length(); idx++){
        loc = pre->at(idx);
        val = chunk->Value(loc-base);
        if (func(val,pred)){
          idx_builder.Append(loc);
        }
      }

    }
  }
}

template<class T>
        IntermediateResult FilterArrowDouble(std::string filename, int col, double pred, T func){
  std::shared_ptr<arrow::Table> table;
  std::vector<int> vect{ col};
  read_feather_column_to_table(filename, &table, vect);
  arrow::Int64Builder builder;
  std::shared_ptr<arrow::Array> array;
  int cnt = 0;
  double val = 0.0;

  auto n_chunk = table->column(0)->num_chunks();

  for (int i=0;i<n_chunk;i++){
    auto target_col =
            std::static_pointer_cast<arrow::DoubleArray>(table->column(0)->chunk(i));

    for (int64_t idx = 0; idx < target_col->length(); idx++){
      val = target_col->Value(idx);
      if (func(val,pred)){
        builder.Append(cnt);
      }
      cnt++;
    }

  }
  if(!builder.Finish(&array).ok())
    throw std::runtime_error("Could not create result array.");

  return IntermediateResult{array};
}


