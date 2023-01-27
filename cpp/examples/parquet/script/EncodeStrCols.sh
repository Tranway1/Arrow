dir="/data/columns/"

for file in $(cat StringCol.txt)
do
  /home/chunwei/arrow/cpp/cmake-build-release-azure/release/col_recode "${dir}${file}.tmp.DICT" uncompressed >> encode.log
done
