//
// Created by Chunwei Liu on 7/27/21.
//

#include <arrow/filesystem/localfs.h>
#include <arrow/ipc/api.h>
#include <parquet/properties.h>
#include <set>
#include <iostream>
#include <fstream>

#ifndef ARROW_V_UTIL_H
#define ARROW_V_UTIL_H

#endif //ARROW_V_UTIL_H


const int64_t ARROW_BATCH_SIZE = 64 * 1024;


std::ifstream::pos_type filesize(std::string filename) {
    std::ifstream in(filename, std::ifstream::ate | std::ifstream::binary);
    return in.tellg();
}


void read_feather_column_to_table(std::string path, std::shared_ptr<arrow::Table> *feather_table,
                                  std::vector<int> &indices) {
    std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
    arrow::fs::LocalFileSystem file_system;
    std::shared_ptr<arrow::io::RandomAccessFile> input_file = file_system.OpenInputFile(path).ValueOrDie();
    std::shared_ptr<arrow::ipc::feather::Reader> feather_reader = arrow::ipc::feather::Reader::Open(
            input_file).ValueOrDie();
    arrow::Status temp_status = feather_reader->Read(indices, feather_table);
//    arrow::Status temp_status = feather_reader->Read( feather_table);
    std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
    auto iotime = std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count();
    std::cout << "time elapsed in io arrow: " << iotime << std::endl;


    end = std::chrono::steady_clock::now();
    auto parsetime = std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count();
    std::cout << "time elapsed in parse arrow table: " << parsetime << std::endl;

    if (temp_status.ok()) {
        std::cout << "Read feather file Successfully." << std::endl;
//        std::cout << "extract schema: "<< ((*feather_table)->schema()) -> ToString() << std::endl; // this line gives segfault
    } else {
        std::cout << "Feather file reading process failed." << std::endl;
    }
    return;
}

void read_feather_to_table(std::string path, std::shared_ptr<arrow::Table> *feather_table) {

    arrow::fs::LocalFileSystem file_system;
    std::shared_ptr<arrow::io::RandomAccessFile> input_file = file_system.OpenInputFile(path).ValueOrDie();
    std::shared_ptr<arrow::ipc::feather::Reader> feather_reader = arrow::ipc::feather::Reader::Open(
            input_file).ValueOrDie();
    arrow::Status temp_status = feather_reader->Read(feather_table);
//    std::cout << "@@@@," << path << "," << filesize(path) << "\n";

    if (temp_status.ok()) {
        std::cout << "Read feather file Successfully." << std::endl;
//    std::cout << ((*feather_table)->schema()) -> ToString() << std::endl; // this line gives segfault
    } else {
        std::cout << "Feather file reading process failed." << std::endl;
    }
    return;
}


void
read_feather_column_to_table(std::string path, std::shared_ptr<arrow::Table> *feather_table, std::vector<int> &indices,
                             std::vector<int> &chunks) {

    arrow::fs::LocalFileSystem file_system;
    std::shared_ptr<arrow::io::RandomAccessFile> input_file = file_system.OpenInputFile(path).ValueOrDie();
    std::shared_ptr<arrow::ipc::feather::Reader> feather_reader = arrow::ipc::feather::Reader::Open(
            input_file).ValueOrDie();
    arrow::Status temp_status = feather_reader->Read(indices, chunks, feather_table);
    if (temp_status.ok()) {
        std::cout << "Read feather file Successfully." << std::endl;
//    std::cout << ((*feather_table)->schema()) -> ToString() << std::endl; // this line gives segfault
    } else {
        std::cout << "Feather file reading process failed." << std::endl;
    }
    return;
}

std::string Get_Parquet_File(std::string f_name, std::string comp, int comp_level) {
    if (comp.find("zstd") != std::string::npos)
        return f_name+"_"+comp+std::to_string(comp_level)+"_v2.parquet";
    return f_name+"_"+comp+"_v2.parquet";
}

std::string Get_Parquet_File(std::string f_name, std::string comp) {
  return f_name+"_"+comp+".parquet";
}

std::string Get_ORC_File(std::string f_name, std::string comp) {
    return f_name + "_" + comp + ".orc";
}


std::string Get_Arrow_File(std::string f_name, std::string comp, int comp_level) {
    if (comp.find("zstd") != std::string::npos)
        return f_name+"_"+comp+std::to_string(comp_level)+".arrow";
    return f_name + "_" + comp + ".arrow";
}

std::string Get_Arrow_File(std::string f_name, std::string comp) {
  return f_name + "_" + comp + ".arrow";
}

// random generator function:
int myrandom(int i) { return std::rand() % i; }


void randperm(int *matrix, int size) {
    std::random_shuffle(matrix, matrix + size);
}


int *genArray0ton(int n) {
    int *arr = new int[n];
    int i = 0;
    for (i = 0; i < n; i++) {
        arr[i] = i;
    }
    return arr;
}


int *getRandomIdx(int n, float r) {
    int num = n * r;
    int *arr = genArray0ton(n);
    randperm(arr, n);
    int *res = new int[num];
    for (int i = 0; i < num; i++) {
        res[i] = arr[i];
//    std::cout << res[i] << " ";
    }
    std::sort(res, res + num);
    std::cout << "res length" << num << std::endl;
//  for (int i = 0; i < num; ++i)
//    std::cout << res[i] << " ";
    return res;
}


std::set<int> extractChunks(int *input, int len, int *converted_idx) {
    std::set<int> chunks_involved;
    int pre_c = -1;
    int cur_c = 0;
    int skipped = 0;
    for (int i = 0; i < len; i++) {
        cur_c = input[i] / ARROW_BATCH_SIZE;
        if (cur_c > pre_c) {
            skipped = skipped + (cur_c - pre_c - 1) * ARROW_BATCH_SIZE;
        }
        chunks_involved.insert(cur_c);
        converted_idx[i] = input[i] - skipped;
//    std::cout<<"pre_c:"<<pre_c<<" cur_c: "<<cur_c<<" Skipped: "<<skipped<<" Skipped chunks: "<<(cur_c-pre_c-1)<<" input: "<<input[i]<<" converted: "<<converted_idx[i]<<std::endl;
        pre_c = cur_c;
    }
    std::cout << "converted chunks:" << chunks_involved.size() << std::endl;
    return chunks_involved;
}




