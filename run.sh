#echo "Run DB_Bench Start ..."
#rm -rf /mnt/ssd/leveldb/*
#rm -rf /mnt/pmem/*
#/home/ununtu25/下载/ThanosKV_origin/ThanosKV_1021/cmake-build-debug/db_bench --db=/mnt/ssd/leveldb --benchmarks=fillrandom,stats --num=50000000 --histogram=1 --write_buffer_size=67108864 --max_file_size=67108864 --histogram=1 > /home/ununtu25/wyy/second_output/CHIMS/twitter_cluster/12.txt 2>&1
#exit 0
#echo "Run DB_Bench Start ..."
#rm -rf /mnt/ssd/leveldb/*
#rm -rf /mnt/pmem/*
#/home/ununtu25/下载/ThanosKV_origin/ThanosKV_1021/cmake-build-debug/db_bench --db=/mnt/ssd/leveldb --benchmarks=fillrandom,stats --num=3571180 --histogram=1 --value_size=15001 --write_buffer_size=67108864 --max_file_size=67108864 --histogram=1 > /home/ununtu25/wyy/second_output/CHIMS/twitter_cluster/37.txt 2>&1
#exit 0
#echo "Run DB_Bench Start ..."
#rm -rf /mnt/ssd/leveldb/*
#rm -rf /mnt/pmem/*
#/home/ununtu25/下载/ThanosKV_origin/ThanosKV_1021/cmake-build-debug/db_bench --db=/mnt/ssd/leveldb --benchmarks=fillrandom,stats --num=20000000 --value_size=1825 --histogram=1 --write_buffer_size=67108864 --max_file_size=67108864 --histogram=1 > /home/ununtu25/wyy/second_output/CHIMS/twitter_cluster/41.txt 2>&1
#exit 0

echo "Run DB_Bench Start ..."
rm -rf /mnt/ssd/leveldb/*
rm -rf /mnt/pmem/*
/home/ununtu25/下载/ThanosKV_origin/ThanosKV_1021/cmake-build-debug/db_bench --db=/mnt/ssd/leveldb --benchmarks=fillrandom,stats --num=50000000 --value_size=63 --histogram=1 --write_buffer_size=33554432 --max_file_size=67108864 --histogram=1 > /home/ununtu25/wyy/second_output/CHIMS/twitter_cluster/48.txt 2>&1
exit 0
