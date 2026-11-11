echo "leveldb ycsb-c start!"
#echo "leveldb ycsb-c 50w-50r running!"
#rm -rf /mnt/ssd/chims/*
#rm -rf /mnt/pmem/*
#./ycsbc -filename /mnt/ssd/chims -db leveldb -configpath 0 -P workloads/UR_workload/workloadh.spec > /home/ununtu25/wyy/second_output/CHIMS/ycsbc/4KB/50w-50r.txt 2>&1
#echo "leveldb ycsb-c 90w-10r running!"
#rm -rf /mnt/ssd/chims/*
#rm -rf /mnt/pmem/*
#./ycsbc -filename /mnt/ssd/chims -db leveldb -configpath 0 -P workloads/UR_workload/workloadi.spec > /home/ununtu25/wyy/second_output/CHIMS/ycsbc/1KB/write_intensive/90w-10r.txt 2>&1
#echo "leveldb ycsb-c 75w-25r running!"
#rm -rf /mnt/ssd/chims/*
#rm -rf /mnt/pmem/*
#./ycsbc -filename /mnt/ssd/chims -db leveldb -configpath 0 -P workloads/UR_workload/workloadj.spec > /home/ununtu25/wyy/second_output/CHIMS/ycsbc/1KB/write_intensive/75w-25r.txt 2>&1
#echo "leveldb ycsb-c 10w-90r running!"
#rm -rf /mnt/ssd/chims/*
#rm -rf /mnt/pmem/*
#./ycsbc -filename /mnt/ssd/chims -db leveldb -configpath 0 -P workloads/UR_workload/workloadul.spec > /home/ununtu25/wyy/second_output/CHIMS/ycsbc/1KB/read_intensive/10w-90r.txt 2>&1
#echo "leveldb ycsb-c 25w-75r running!"
#rm -rf /mnt/ssd/chims/*
#rm -rf /mnt/pmem/*
#./ycsbc -filename /mnt/ssd/chims -db leveldb -configpath 0 -P workloads/UR_workload/workloaduk.spec > /home/ununtu25/wyy/second_output/CHIMS/ycsbc/1KB/read_intensive/25w-75r.txt 2>&1
##echo "leveldb ycsb-c e running!"
#rm -rf /mnt/ssd/chims/*
#rm -rf /mnt/pmem/*
#./ycsbc -filename /mnt/ssd/chims -db leveldb -configpath 0 -P workloads/workloade1.spec > /home/ununtu25/wyy/second_output/CHIMS/ycsbc/1KB/scan/1kb.txt 2>&1

#echo "leveldb ycsb-c H running!"
#rm -rf /mnt/ssd/chims/*
#rm -rf /mnt/pmem/*
#./ycsbc -filename /mnt/ssd/leveldb -db leveldb -configpath 0 -P workloads/UR_workload/workloadi.spec > /home/ununtu25/wyy/second_output/CHIMS/zip_const/write_intensive/1.1_90w_10r.txt 2>&1
#echo "leveldb ycsb-c J running!"
#rm -rf /mnt/ssd/chims/*
#rm -rf /mnt/pmem/*
#./ycsbc -filename /mnt/ssd/chims -db leveldb -configpath 0 -P workloads/UR_workload/workloadul.spec > /home/ununtu25/wyy/second_output/CHIMS/zip_const/read_intensive/1.1_10w_90r.txt 2>&1
#exit 0
echo "leveldb ycsb-c 10w-90r running!"
rm -rf /mnt/ssd/leveldb/*
rm -rf /mnt/pmem/*
./ycsbc -filename /mnt/ssd/leveldb -db leveldb -configpath 0 -P workloads/UR_workload/workloadul.spec > /home/ununtu25/wyy/second_output/extend_experiment/不同模块的影响/NoDRAM/10w-90r.txt 2>&1
echo "leveldb ycsb-c 90w-10r running!"
rm -rf /mnt/ssd/leveldb/*
rm -rf /mnt/pmem/*
./ycsbc -filename /mnt/ssd/leveldb -db leveldb -configpath 0 -P workloads/UR_workload/workloadi.spec > /home/ununtu25/wyy/second_output/extend_experiment/不同模块的影响/NoDRAM/90w-10r.txt 2>&1
echo "leveldb ycsb-c 50w-50r running!"
rm -rf /mnt/ssd/leveldb/*
rm -rf /mnt/pmem/*
./ycsbc -filename /mnt/ssd/leveldb -db leveldb -configpath 0 -P workloads/UR_workload/workloadh.spec > /home/ununtu25/wyy/second_output/extend_experiment/不同模块的影响/NoDRAM/50w-50r.txt 2>&1
echo "leveldb ycsb-c success!"
