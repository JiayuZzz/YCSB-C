//
// Created by wujy on 18-1-21.
//

#include "leveldb_db.h"
#include <iostream>

using namespace std;

namespace ycsbc {
    LevelDB::LevelDB(const char *dbfilename) :noResult(0){
        leveldb::Options options;
        options.create_if_missing = true;
        options.compression = leveldb::kNoCompression;

        leveldb::Status s = leveldb::DB::Open(options,dbfilename,&db_);
        if(!s.ok()){
            cerr<<"Can't open leveldb "<<dbfilename<<endl;
            exit(0);
        }
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
        int cnt=0;
        for(KVPair p:values){
            s = db_->Put(leveldb::WriteOptions(),key,p.second);
            ++cnt;
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
        leveldb::Status s = db_->Delete(leveldb::WriteOptions(),key);
        if(!s.ok()){
            cerr<<"delete error"<<endl;
            exit(0);
        }
        return DB::kOK;
    }

    LevelDB::~LevelDB() {
        delete db_;
    }
}