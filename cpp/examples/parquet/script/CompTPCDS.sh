dir="/mnt/dataset/"
#for comp in uncompressed zstd lz4 gzip snappy;
for comp in snappy;
do
	for table in customer_demographics customer customer_address item inventory store_sales store_returns catalog_returns catalog_sales web_sales web_returns;
		do
		  if [[ $comp == "zstd" ]]
		  then
#		    for level in 1 5 9;
		    for level in 9;
		    do
		       /home/chunweiliu/arrow/cpp/cmake-build-release-azurevm/release/parquet_arrow_tpsds_example "${dir}${table}" ${comp} ${level}
		    done
		  else
		    /home/chunweiliu/arrow/cpp/cmake-build-release-azurevm/release/parquet_arrow_tpsds_example "${dir}${table}" ${comp}
      fi
		done
done