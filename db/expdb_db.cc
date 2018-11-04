//
// Created by wujy on 9/10/18.
//

#include "expdb_db.h"
#include <iostream>
#include "leveldb/filter_policy.h"
#include "leveldb/cache.h"
#include "leveldb_config.h"

using namespace std;

namespace ycsbc {
    LevelDBExp::LevelDBExp(const char *dbfilename) :noResult(0){
        //get leveldb config
        ConfigLevelDB config = ConfigLevelDB();
        int bloomBits = config.getBloomBits();
        bool seekCompaction = config.getSeekCompaction();
        bool compression = config.getCompression();
        bool directIO = config.getDirectIO();
        int expThreads = config.getExpThreads();
        int sizeRatio = config.getSizeRatio();
        string vlogDir = config.getVlogDir();
        size_t blockCache = config.getBlockCache();
        size_t memtableSize = config.getExpdbMem();
        size_t gcSize = config.getGcSize();
        //set options
        leveldb::ExpOptions options;
        options.create_if_missing = true;
        if(!compression)
            options.compression = leveldb::kNoCompression;
        if(bloomBits>0)
            options.filter_policy = leveldb::NewBloomFilterPolicy(bloomBits);
        options.exp_ops.seekCompaction = seekCompaction;
        options.exp_ops.directIO = directIO;
        options.exp_ops.sizeRatio = sizeRatio;
        options.gcAfterExe = gcSize;
        options.numThreads = expThreads;
        options.block_cache = leveldb::NewLRUCache(blockCache);
        options.vlogMemSize = memtableSize;

        leveldb::Status s = leveldb::ExpDB::Open(options,dbfilename,vlogDir,&db_);
        cerr<<vlogDir<<endl;
        if(!s.ok()){
            cerr<<"Can't open leveldb "<<dbfilename<<endl;
            exit(0);
        }
        cout<<"\nbloom bits:"<<bloomBits<<"bits\ndirectIO:"<<(bool)directIO<<"\nseekCompaction:"<<(bool)seekCompaction<<endl;
    }

    int LevelDBExp::Read(const std::string &table, const std::string &key, const std::vector<std::string> *fields,
                          std::vector<KVPair> &result) {
        string value;
        leveldb::Status s = db_->Get(leveldb::ReadOptions(),key,&value);
        if(s.ok()) return DB::kOK;
        if(s.IsNotFound()){
            noResult++;
            return DB::kOK;
        }else{
            cerr<<"read error"<<endl;
            exit(0);
        }
    }


    int LevelDBExp::Scan(const std::string &table, const std::string &key, int len, const std::vector<std::string> *fields,
                          std::vector<std::vector<KVPair>> &result) {
        std::vector<std::string> keys(len);
        std::vector<std::string> vals(len);
        db_->Scan(leveldb::ReadOptions(),key,len,keys,vals);
	/*
        for(string& s:vals){
            cerr<<s<<endl;
        }
	*/
        return DB::kOK;
    }

    int LevelDBExp::Insert(const std::string &table, const std::string &key,
                            std::vector<KVPair> &values){
        leveldb::Status s;
        for(KVPair &p:values){
            s = db_->Put(leveldb::WriteOptions(),key,p.second);
            if(!s.ok()){
                cerr<<"insert error\n"<<endl;
                exit(0);
            }
        }
        return DB::kOK;
    }

    int LevelDBExp::Update(const std::string &table, const std::string &key, std::vector<KVPair> &values) {
        return Insert(table,key,values);
    }

    int LevelDBExp::Delete(const std::string &table, const std::string &key) {
        vector<DB::KVPair> values;
        return Insert(table,key,values);
    }

    void LevelDBExp::printStats() {
        string stats;
        db_->GetProperty("leveldb.stats",&stats);
        cout<<stats<<endl;
        return;
    }

    LevelDBExp::~LevelDBExp() {
        std::cerr<<"not found:"<<noResult<<std::endl;
        delete db_;
    }
}
