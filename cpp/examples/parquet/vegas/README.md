#Vegas vNext Arrow API

This custom arrow/parquet API supports efficient loading, including target column only extraction and conversion (Parquet to Arrows), target column chunk loading, data skipping, and direct query on dictionary encoded data.
Please build the project with
    
    _/usr/bin/cmake -DCMAKE_BUILD_TYPE=Release DARROW_BUILD_EXAMPLES=ON -DPARQUET_BUILD_EXAMPLES=ON -DARROW_PARQUET=ON -DARROW_DATASET=ON -DARROW_ORC=ON -DCMAKE_DEPENDS_USE_COMPILER=FALSE -G "CodeBlocks - Unix Makefiles" /home/chunweiliu/arrow/cpp_
to include axamples and enable compressions for Arrow.


We keep modifications on Arrow standard API as little as possible. Most new interfaces are included in v_unit.h, arrow_helper.h, parquet_helper.h.
And Vegas vNext scan operator logics are integrated into scan_operator.cpp with five hand-written query stems.

The folder also includes some encoding experiments, including Parquet and Arrow encoding/compression performance, compression ratio, transcoding overhead evaluation.
It also includes query micro-benchmarking, including filtering, projection with/without changing selectivity, direct query evaluations.

* _parquet_arrow_tpsds_example_ (vegas/tpcds_reader_writer.cc) includes code blocks that encode the text tpc-ds file into parquet,
transcode parquet into arrow with given compression and compression level (compression level is now only working for zstd), some filtering, projection and aggregation queries. 
Since the Parquet encoding and Parquet->Arrow transcoding usually takes long time, you can manually disable those code blocks to save time if you have encoded file in your local folder.
Run example with given [file], [compression] and [compression_level]: _/release/parquet_arrow_tpsds_example /mnt/dataset/catalog_sales lz4_

* _parquet_arrow_projection_example_ (vegas/projection_example.cc) inlcudes micro-benchmarking on Parquet and Arrow with varying selectivity.
Run example with given [file], [compression] and [selectivity]: _/release/parquet_arrow_projection_example /mnt/dataset/catalog_sales lz4 0.1_

* _parquet_arrow_vegas_ (vegas/gandiva_example.cpp) includes a simple example showing how gandiva expression work on Parquet and Arrow files

* _parquet_arrow_dictionary_example_ (vegas/dictionary_filtering_example.cc) run arrow direct query on the encoded domain.
  Run example with given [file], [compression]: _/release/parquet_arrow_dictionary_example /mnt/dataset/catalog_sales lz4 0.1_

* _scan_example_ (vegas/scan_operator.cpp) includes filtering-projection query evaluations with 5 query stems from tpd-ds workload.


## Query-related Columns Extraction for Parquet and Arrow
Many cloud DBMSs load the whole file into their in-memory representation before executing the input query. For example, Spark loads parquet data into DataFrame before the query execution.
Loading the whole table into memory involves more data parsing, decompression, and decoding cost.
So in our Vegas vNext, we use a smart loading where we only deserialize the related data column into in-memory representation for query execution.
This requires Parquet/Arrow reader to extract only target columns from each row group / data chunk. You can find related code from _void read_feather_column_to_table(std::string path,std::shared_ptr\<arrow::Table\> *feather_table, std::vector<int>& indices)_ in v_util.h
## Data Skipping in DataChunk Level
In addition to the target columns only extraction, we also implement an efficient data skipping strategy for Parquet and Arrow.
With a given qualified bit-vector and Arrow batch size, we can calculate the qualified data chunks involved.
So we add a new interface to extract the target data chunks only according to the input bit-vector. This is delivered by
_Status Read(const std::vector<int>& indices, const std::vector<int>& chunks,std::shared_ptr\<Table\>* out)_ in feather.h.
Related logics involve indices mapping (_std::set\<int\> extractChunks(int* input, int len, int* converted_idx_ ) in v_util.h)

## Direct Query On Dictionary Encoding
Arrow has minimal encoding options compared with Parquet, where dictionary encoding, Run-length-encoding, bit-packing and their hybrid combinations are widely used.
Arrow dictionary encoding is eligible for string and binary type only, and it is not enabled by default. In addition, the encoded dictionary key is usually saved with int32/64.
So dictionary encoding compresses the data only if the given attributes has a length greater than 32 bits. In our code,
we force dictionary encoding on string columns by setting _parquet::ArrowReaderProperties_ with _set_read_dictionary_ enabled for target columns,
and passing the property instance to arrow reader with _Status OpenFile(std::shared_ptr<::arrow::io::RandomAccessFile> file, MemoryPool* pool,
std::unique_ptr<FileReader>* reader,ArrowReaderProperties& arg_properties)_ in reader.h. Example is inlcuded in _Parquet2ArrowDict()_.


We force the arrow reader to postpone the decoding process to push the query further through the data loading stack.
For each data chunk, we extract the dictionary first. Then, we translate the original string domain to an encoded integer domain (_DictTranslate()_).
With this translation, we can first avoid decoding and transform the string comparison to int comparison (example in _FilterDictStringChunk()_). Direct query on String attributes can be further boosted with arrow SIMD support.
Besides, more queries can be supported with order-preserving dictionary encoding, which can be supported by arrow.

###Contributor
Chunwei, Brandon
