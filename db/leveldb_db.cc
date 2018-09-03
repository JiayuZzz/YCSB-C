//
// Created by wujy on 18-1-21.
//

#include "leveldb_db.h"
#include <iostream>
#include "leveldb/filter_policy.h"

using namespace std;

namespace ycsbc {
    LevelDB::LevelDB(const char *dbfilename) :noResult(0){
        //get leveldb config
        ConfigLevelDB config = ConfigLevelDB();
        int bloomBits = config.getBloomBits();
        bool seekCompaction = config.getSeekCompaction();
        bool compression = config.getCompression();
        bool directIO = config.getDirectIO();
        //set options
        leveldb::Options options;
        options.create_if_missing = true;
        if(!compression)
            options.compression = leveldb::kNoCompression;
        if(bloomBits>0)
            options.filter_policy = leveldb::NewBloomFilterPolicy(bloomBits);
        options.exp_ops.seekCompaction = seekCompaction;
        options.exp_ops.directIO = directIO;

        leveldb::Status s = leveldb::DB::Open(options,dbfilename,&db_);
        if(!s.ok()){
            cerr<<"Can't open leveldb "<<dbfilename<<endl;
            exit(0);
        }
        cout<<"\nbloom bits:"<<bloomBits<<"bits\ndirectIO:"<<(bool)directIO<<"\nseekCompaction:"<<(bool)seekCompaction<<endl;
    }

    int LevelDB::Read(const std::string &table, const std::string &key, const std::vector<std::string> *fields,
                      std::vector<KVPair> &result) {
        string value;
        leveldb::Status s = db_->Get(leveldb::ReadOptions(),key,&value);
        if(s.ok()) return DB::kOK;
        if(s.IsNotFound()){
            noResult++;
            cout<<noResult<<endl;
            return DB::kOK;
        }else{
            cerr<<"read error"<<endl;
            exit(0);
        }
    }


    int LevelDB::Scan(const std::string &table, const std::string &key, int len, const std::vector<std::string> *fields,
                      std::vector<std::vector<KVPair>> &result) {
        leveldb::Iterator* it=db_->NewIterator(leveldb::ReadOptions());
        string value;
        for(it->Seek(key);len>0&&it->Valid();it->Next()){
                value = it->key().ToString();
            }
        if(!it->Valid()){
            cerr<<"scan error"<<endl;
            exit(0);
        }
        return DB::kOK;
    }

    int LevelDB::Insert(const std::string &table, const std::string &key,
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

    int LevelDB::Update(const std::string &table, const std::string &key, std::vector<KVPair> &values) {
        return Insert(table,key,values);
    }

    int LevelDB::Delete(const std::string &table, const std::string &key) {
        vector<DB::KVPair> values;
        return Insert(table,key,values);
    }

    void LevelDB::printStats() {
        string stats;
        db_->GetProperty("leveldb.stats",&stats);
        cout<<stats<<endl;
    }

    LevelDB::~LevelDB() {
        delete db_;
    }
}
