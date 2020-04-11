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
#include <vector>
#include <future>
#include "core/utils.h"
#include "core/timer.h"
#include "core/client.h"
#include "core/core_workload.h"
#include "db/db_factory.h"
#include "unistd.h"

using namespace std;

void UsageMessage(const char *command);
bool StrStartWith(const char *str, const char *pre);
string ParseCommandLine(int argc, const char *argv[], utils::Properties &props);
double ops_time[4]={0.0};
long ops_cnt[4]={0};

int DelegateClient(ycsbc::DB *db, ycsbc::CoreWorkload *wl, const int num_ops,
    bool is_loading) {
  db->Init();
  ycsbc::Client client(*db, *wl);
  int oks = 0;
  if(is_loading){
    cerr << "loading "<<num_ops<<" records..."<<endl;
  } else {
    cerr << "running on "<<num_ops<<" records..."<<endl;
  }
  for (int i = 0; i < num_ops; ++i) {
    if(i%10000==0){
      cerr << "finished ops: "<<i<<"\r";
    }
    if (is_loading) {
      oks += client.DoInsert();
    } else {
      oks += client.DoTransaction();
    }
  }
  cerr << endl;
  db->Close();
  return oks;
}

int main(const int argc, const char *argv[]) {
  utils::Properties props;
  string file_name = ParseCommandLine(argc, argv, props);

  ycsbc::DB *db = ycsbc::DBFactory::CreateDB(props);
  if (!db) {
    cout << "Unknown database name " << props["dbname"] << endl;
    exit(0);
  }

  vector<future<int>> actual_ops;
  int total_ops;
  utils::Timer timer;
  // bool skipLoad = utils::StrToBool(props["skipLoad"]);
  std::string phase = props["phase"];
  std::cerr<<"threads "<<props["threadcount"]<<std::endl;

  const int num_threads = stoi(props.GetProperty("threadcount", "1"));
  const int s = stoi(props.GetProperty("sleep", "0"));

  // Loads data
  if(phase == "load" || phase == "both") {
    ycsbc::CoreWorkload wl;
    wl.Init(props);
    timer.Start();
    total_ops = stoi(props[ycsbc::CoreWorkload::RECORD_COUNT_PROPERTY]);
    for (int i = 0; i < num_threads; ++i) {
      actual_ops.emplace_back(async(launch::async,
                                    DelegateClient, db, &wl, total_ops / num_threads, true));
    }
    assert((int) actual_ops.size() == num_threads);

    int sum = 0;
    for (auto &n : actual_ops) {
      assert(n.valid());
      sum += n.get();
    }
    cout << "# Loading records:\t" << sum << endl;
    cout << "Load time: "<<timer.End()/1000000<<"s"<<endl;
    actual_ops.clear();
    //cerr<< "done, sleep 10 minutes for compaction"<<endl;
    cout << "Read ops： " << ops_cnt[ycsbc::READ] << "\nTotal read time: " << ops_time[ycsbc::READ]/1000000 << "s" <<endl;
    cout << "Time per read: " << ops_time[ycsbc::READ]/ops_cnt[ycsbc::READ]/1000 << "ms" <<endl;
    cout << "Insert ops: " << ops_cnt[ycsbc::INSERT] << "\nTotal insert time: " << ops_time[ycsbc::INSERT]/1000000 << "s" <<endl;
    cout << "Time per insert: " << ops_time[ycsbc::INSERT]/ops_cnt[ycsbc::INSERT]/1000 << "ms" <<endl;
    cout << "Scan ops: " << ops_cnt[ycsbc::SCAN] << "\nTotal scan time: "<< ops_time[ycsbc::SCAN]/1000000 << "s" <<endl;
    cout << "Time per scan: " << ops_time[ycsbc::SCAN]/ops_cnt[ycsbc::SCAN]/1000 << "ms" <<endl;
    if (props["dbname"] == "leveldb"||props["dbname"] == "vlog"||props["dbname"]=="expdb"||props["dbname"]=="rocksdb"||props["dbname"]=="titandb"||props["dbname"]=="vtable"){
      cout << "============================statistics==========================="<<endl;
      db->printStats();
    }
    for(int i=0;i<4;i++){
      ops_cnt[i] = 0;
      ops_time[i] = 0;
    }
  } 
  if (phase == "run" || phase == "both") {
    ycsbc::CoreWorkload wl;
    wl.Init(props, true/*run_phase*/);
    // Performs transactions
    total_ops = stoi(props[ycsbc::CoreWorkload::OPERATION_COUNT_PROPERTY]);
    timer.Start();
    for (int i = 0; i < num_threads; ++i) {
      actual_ops.emplace_back(async(launch::async,
                                    DelegateClient, db, &wl, total_ops / num_threads, false));
    }
    assert((int) actual_ops.size() == num_threads);

    int sum = 0;
    for (auto &n : actual_ops) {
      assert(n.valid());
      sum += n.get();
    }
    double duration = timer.End();
    cout << "# Transaction throughput (KTPS)" << endl;
    cout << props["dbname"] << '\t' << file_name << '\t' << num_threads << '\t';
    cout << total_ops / duration / 1000000 / 1000 << endl;
    cout << "run time: " << duration << "us\n\n" << endl;

    cout << "Read ops： " << ops_cnt[ycsbc::READ] << "\nTotal read time: " << ops_time[ycsbc::READ]/1000000 << "s" <<endl;
    cout << "Time per read: " << ops_time[ycsbc::READ]/ops_cnt[ycsbc::READ]/1000 << "ms" <<endl;
    cout << "Insert ops: " << ops_cnt[ycsbc::INSERT] << "\nTotal insert time: " << ops_time[ycsbc::INSERT]/1000000 << "s" <<endl;
    cout << "Time per insert: " << ops_time[ycsbc::INSERT]/ops_cnt[ycsbc::INSERT]/1000 << "ms" <<endl;
    cout << "Scan ops: " << ops_cnt[ycsbc::SCAN] << "\nTotal scan time: "<< ops_time[ycsbc::SCAN]/1000000 << "s" <<endl;
    cout << "Time per scan: " << ops_time[ycsbc::SCAN]/ops_cnt[ycsbc::SCAN]/1000 << "ms" <<endl;
   // if (props["dbname"] == "leveldb"||props["dbname"] == "vlog"||props["dbname"]=="expdb"||props["dbname"]=="rocksdb"||props["dbname"]=="titandb"||props["dbname"]=="vtable"){
      cout << "============================statistics==========================="<<endl;
      db->printStats();
    //}
	/*
    if(phase=="both"){
      cout<<"sleep 20m for compaction complete"<<endl;
      sleep(1200);
      db->printStats();
    }
	*/
    for(int i=0;i<4;i++){
      ops_cnt[i] = 0;
      ops_time[i] = 0;
    }
  }
    if(s>0){
	  std::cout<<"sleep "<<s<<"s for compaction"<<std::endl;
      sleep(s);
      db->printStats();
    }
  
  std::cerr<<"delete db"<<std::endl;
  delete db;
  std::cerr<<"deleted db"<<std::endl;
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
    } else if(strcmp(argv[argindex],"-skipLoad")==0){
      argindex++;
      if(argindex >= argc){
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("skipLoad",argv[argindex]);
      argindex++;
    } else if(strcmp(argv[argindex],"-phase")==0){
      argindex++;
      if(argindex >= argc){
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("phase",argv[argindex]);
      argindex++;
    } else if(strcmp(argv[argindex],"-sleep")==0){
      argindex++;
      if(argindex >= argc){
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("sleep",argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-slaves") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("slaves", argv[argindex]);
      argindex++;
    } else if(strcmp(argv[argindex],"-dbfilename")==0){
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("dbfilename", argv[argindex]);
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

