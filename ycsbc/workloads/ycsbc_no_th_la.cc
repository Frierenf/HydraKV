//
//  ycsbc.cc
//  YCSB-C
//
//  Created by Jinglei Ren on 12/19/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include <cstring>
#include <string>
#include <iostream>
#include <sstream>
#include <fstream>
#include <unistd.h>
#include <vector>
#include <future>
#include <unistd.h>
#include "core/utils.h"
#include "core/timer.h"
#include "core/client.h"
#include "core/core_workload.h"
#include "db/db_factory.h"

using namespace std;

// // ======================================================
// std::string thoughput_load_fname = "/mnt/thoughput/novelsm_throughput_load_";
// std::string thoughput_trans_fname = "/mnt/thoughput/novelsm_throughput_trans_";
// std::string filetype = ".csv";
// // ======================================================


void UsageMessage(const char *command);
bool StrStartWith(const char *str, const char *pre);
string ParseCommandLine(int argc, const char *argv[], utils::Properties &props);
struct response_times{
	double** res_times;
};
int DelegateClient(ycsbc::DB *db, ycsbc::CoreWorkload *wl, const int num_ops,
    bool is_loading, int num_thread_index, response_times* rts) {
  db->Init();
  ycsbc::Client client(*db, *wl);
  int oks = 0;
  int next_report_=100;
  rts->res_times[num_thread_index]=new double[num_ops];
  
  // // =====================================================
  // // init output file
  // std::ostringstream oss;
  // std::string fname = "";
  // if (is_loading) {
  //   oss << thoughput_load_fname << num_ops << filetype;
  //   fname = oss.str();
  // } else {
  //   oss << thoughput_trans_fname << num_ops << filetype;
  //   fname = oss.str();
  // }
  // ofstream outFile(fname.c_str(), ios::out);

  // // init
  // int value_size = wl->ValueSize();

  // utils::Timer<double> last_time;
  // last_time.Start();
  // uint64_t op_count = 0;
  // // =====================================================
  
  for (int i = 0; i < num_ops; ++i) {
    if (is_loading) {
    	//  utils::Timer<double> response_load;
    	//  response_load.Start();
      oks += client.DoInsert();
      // double response_time=response_load.End();
      // rts->res_times[num_thread_index][i]=response_time*1000;
    } else {
    	// utils::Timer<double> response_load;
    	// response_load.Start();
      oks += client.DoTransaction();
      // double response_time=response_load.End();
      // rts->res_times[num_thread_index][i]=response_time*1000;
    }
	  if ( oks >= next_report_) {
      if      (next_report_ < 1000)   next_report_ += 100;
      else if (next_report_ < 5000)   next_report_ += 500;
      else if (next_report_ < 10000)  next_report_ += 1000;
      else if (next_report_ < 50000)  next_report_ += 5000;
      else if (next_report_ < 100000) next_report_ += 10000;
      else if (next_report_ < 500000) next_report_ += 50000;
      else                            next_report_ += 100000;
      fprintf(stderr, "... finished %d ops%30s\r", oks, "");
      fflush(stderr);
    }
    // // =====================================================
    // op_count++;
    // while (last_time.End() > 1){
    //   // MB/s
    //   double volume = op_count*(value_size) / 1048576.0;
    //   outFile << volume <<endl;
    //   op_count = 0;
    //   last_time.Reset();
    // }
    // // ====================================================

  }

  db->Close();
  return oks;
}

int main(const int argc, const char *argv[]) {
	std::string cmd="cat /proc/";
	FILE *fp;
	char buf[1024];
	char cpu[5];
	long int user,nice,sys,idle,iowait,irq,softirq;
	long int all1,all2,idle1,idle2;
	float usage;
	fp = fopen("/proc/stat","r");
	if(fp == NULL){
		perror("fopen");
		exit(0);
	}
	fgets(buf, sizeof(buf), fp);
#if _DEBUG_
	 printf("buf=%s", buf);
#endif
	sscanf(buf,"%s%ld%ld%ld%ld%ld%ld%ld", cpu, &user, &nice, &sys, &idle, &iowait, &irq, &softirq);//不用担心数据量过大的问题，再大也能解析出来
	all1=user+nice+sys+idle+iowait+irq+softirq;
	idle1=idle;
	rewind(fp);

	int pid_=getpid();
	char connectString_[10];
	sprintf(connectString_,"%d",pid_);
	std::string cmd_result=cmd+connectString_+"/io";
	const char *result=cmd_result.c_str();

	system("cat /proc/stat|grep \"cpu \"|awk \'{for(i=2;i<=NF;i++)j+=$i;print \"cpu_total_slice \" j;}\'");
	std::string cpuprocess=cmd+connectString_+"/stat|awk \'{print \"cpu_process_total_slice \" $14+$15+$16+$17}\'";
	system(cpuprocess.c_str());

  utils::Properties props;
  string file_name = ParseCommandLine(argc, argv, props);

  ycsbc::DB *db = ycsbc::DBFactory::CreateDB(props);
  if (!db) {
    cout << "Unknown database name " << props["dbname"] << endl;
    exit(0);
  }

  ycsbc::CoreWorkload wl;
  wl.Init(props);

  const int num_threads = stoi(props.GetProperty("threadcount", "1"));

  // Loads data
  vector<future<int>> actual_ops;
  int load_total_ops = stoi(props[ycsbc::CoreWorkload::RECORD_COUNT_PROPERTY]);
  response_times rts,lrts;
  rts.res_times=new double*[num_threads];
  lrts.res_times=new double*[num_threads];
  utils::Timer<double> timer_load;
  timer_load.Start();
  for (int i = 0; i < num_threads; ++i) {
    actual_ops.emplace_back(async(launch::async,
        DelegateClient, db, &wl, load_total_ops / num_threads, true, i,&lrts));
  }
  assert((int)actual_ops.size() == num_threads);

  int sum = 0;
  for (auto &n : actual_ops) {
    assert(n.valid());
    sum += n.get();
  }
  double duration_load = timer_load.End();
  cerr << "# Transaction throughput (KTPS)" << endl;
  cerr << props["dbname"] << '\t' << file_name << '\t' << num_threads << '\t';
  cerr << load_total_ops / duration_load / 1000 << endl;
  cerr << "# Loading records:\t" << sum << endl;
  cerr << "load duration:\t" << duration_load << endl;

//sleep(300);

  // Peforms transactions
  actual_ops.clear();
  int total_ops = stoi(props[ycsbc::CoreWorkload::OPERATION_COUNT_PROPERTY]);

  utils::Timer<double> timer;
  timer.Start();
  for (int i = 0; i < num_threads; ++i) {
    actual_ops.emplace_back(async(launch::async,
        DelegateClient, db, &wl, total_ops / num_threads, false, i,&rts));
  }
  assert((int)actual_ops.size() == num_threads);

  sum = 0;
  for (auto &n : actual_ops) {
    assert(n.valid());
    sum += n.get();
  }
  double duration = timer.End();
  cerr << "# Transaction throughput (KTPS)" << endl;
  cerr << props["dbname"] << '\t' << file_name << '\t' << num_threads << '\t';
  cerr << total_ops / duration / 1000 << endl;
  cerr << "performs transactions duration:\t" << duration << endl;

  memset(buf,0,sizeof(buf));
  cpu[0]='\0';
  user=nice=sys=idle=iowait=irq=softirq=0;
  fgets(buf,sizeof(buf),fp);
#if _DEGUG_
  printf("buf=%s", buf);
#endif
  sscanf(buf,"%s%ld%ld%ld%ld%ld%ld%ld", cpu, &user, &nice, &sys, &idle, &iowait, &irq, &softirq);//不用担心数据量过大的问题，再大也能解析出来
  all2=user+nice+sys+idle+iowait+irq+softirq;
  idle2=idle;
  usage = all2-all1;
  usage = (usage-(idle2-idle1))/usage;
  printf("--------------------我自己的cpu使用率计算方法---------------------------");
  printf("all=%ld\n",all2-all1);
  printf("usage=%ld\n",all2-all1-(idle2-idle1));
  printf("cpu use = %.2f\%\n",usage);
  printf("--------------------我自己的cpu使用率计算方法---------------------------");
  fclose(fp);
  system(result);

  system("cat /proc/stat|grep \"cpu \"|awk \'{for(i=2;i<=NF;i++)j+=$i;print \"cpu_total_slice \" j;}\'");
  system(cpuprocess.c_str());
  system("ps aux | grep db_bench");
  
  db->PrintMyStats();

  // for(int i=0; i<num_threads;i++){
	//   std::ostringstream oss;
	//   std::string base_name="/mnt/novelsm_response_times_";
	//   std::string filetype=".csv";
	//   std::string fname="";
	//   oss<<base_name<<i<<filetype;
	//   fname=oss.str();
	//   ofstream outFile1(fname.c_str(), ios::out);

	//   std::ostringstream ossl;
	//   std::string base_namel="/mnt/novelsm_load_response_times_";
	//   std::string filetypel=".csv";
	//   std::string fnamel="";
	//   ossl<<base_namel<<i<<filetypel;
	//   fnamel=ossl.str();
	//   ofstream outFile2(fnamel.c_str(), ios::out);
  //   if(!outFile1.good()){
  //     cerr<<"输出文件1打开失败"<<endl;
  //   }
  //   if(!outFile2.good()){
  //     cerr<<"输出文件2打开失败"<<endl;
  //   }
  //   for(int j=0;j<total_ops / num_threads;j++){
  //     outFile1<<rts.res_times[i][j]<<std::endl;
  //   }
  //   for(int j=0;j<load_total_ops / num_threads;j++){
  //     outFile2<<lrts.res_times[i][j]<<std::endl;
  //   }
  //   outFile1.close();
  //   outFile2.close();
  // }
  
  for(int i=0;i<num_threads;i++){
    delete [] lrts.res_times[i];
    delete [] rts.res_times[i];
  }
  delete[] lrts.res_times;
  delete[] rts.res_times;

}

string ParseCommandLine(int argc, const char *argv[], utils::Properties &props) {
  int argindex = 1;
  string filename;
  while (argindex < argc && StrStartWith(argv[argindex], "-")) {
    if (strcmp(argv[argindex], "-threads") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("threadcount", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-db") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("dbname", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-host") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("host", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-port") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("port", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-slaves") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("slaves", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-filename") == 0) {
        argindex++;
        if (argindex >= argc) {
          UsageMessage(argv[0]);
          exit(0);
        }
        props.SetProperty("dbfilename", argv[argindex]);
        argindex++;
    } else if (strcmp(argv[argindex], "-configpath") == 0) {
         argindex++;
         if (argindex >= argc) {
           UsageMessage(argv[0]);
           exit(0);
         }
         props.SetProperty("configpath", argv[argindex]);
         argindex++;
    } else if (strcmp(argv[argindex], "-P") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      filename.assign(argv[argindex]);
      ifstream input(argv[argindex]);
      try {
        props.Load(input);
      } catch (const string &message) {
        cout << message << endl;
        exit(0);
      }
      input.close();
      argindex++;
    } else {
      cout << "Unknown option '" << argv[argindex] << "'" << endl;
      exit(0);
    }
  }

  if (argindex == 1 || argindex != argc) {
    UsageMessage(argv[0]);
    exit(0);
  }

  return filename;
}

void UsageMessage(const char *command) {
  cout << "Usage: " << command << " [options]" << endl;
  cout << "Options:" << endl;
  cout << "  -threads n: execute using n threads (default: 1)" << endl;
  cout << "  -db dbname: specify the name of the DB to use (default: basic)" << endl;
  cout << "  -P propertyfile: load properties from the given file. Multiple files can" << endl;
  cout << "                   be specified, and will be processed in the order specified" << endl;
}

inline bool StrStartWith(const char *str, const char *pre) {
  return strncmp(str, pre, strlen(pre)) == 0;
}

