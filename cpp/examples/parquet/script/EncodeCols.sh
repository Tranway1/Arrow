dir="/data/columns/"

for file in ${dir}*DICT
do
  /home/chunwei/arrow/cpp/cmake-build-release-azure/release/col_recode "$file" lz4 >> encode.log
done
