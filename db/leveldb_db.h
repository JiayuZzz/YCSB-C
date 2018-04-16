//
// Created by wujy on 18-1-21.
//

#ifndef YCSB_C_LEVELDB_DB_H
#define YCSB_C_LEVELDB_DB_H

#include "leveldb/db.h"
#include "core/db.h"
#include <string>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/ini_parser.hpp>
#include <sstream>
#include <vector>

using std::string;
using std::vector;

namespace ycsbc {
    class LevelDB : public DB{
    public:
        LevelDB();
        int Read(const std::string &table, const std::string &key,
                 const std::vector<std::string> *fields,
                 std::vector<KVPair> &result);

        int Scan(const std::string &table, const std::string &key,
                 int len, const std::vector<std::string> *fields,
                 std::vector<std::vector<KVPair>> &result);

        int Insert(const std::string &table, const std::string &key,
                   std::vector<KVPair> &values);

        int Update(const std::string &table, const std::string &key,
                   std::vector<KVPair> &values);


        int Delete(const std::string &table, const std::string &key);

        void printStats();

        ~LevelDB();

    private:
        leveldb::DB *db_;
        unsigned noResult;
    };

    class ConfigLevelDB {
    private:
        boost::property_tree::ptree pt_;
        int bloomBits_;
        bool seekCompaction_;
        bool compression_;
        bool directIO_;
        int diskNum_;
        vector<string> dbnames_;

    public:
        ConfigLevelDB(){
            boost::property_tree::ini_parser::read_ini("./configDir/leveldb_config.ini",pt_);
            bloomBits_=pt_.get<int>("config.bloombits");
            seekCompaction_=pt_.get<bool>("config.seekcompaction");
            compression_=pt_.get<bool>("config.compression");
            directIO_=pt_.get<bool>("config.directio");
            diskNum_=pt_.get<int>("config.disknum");
            for(int i = 0;i<diskNum_;i++){
                string cfg = "config.dbname";
                std::ostringstream oss;
                oss<<cfg<<i;
                cfg = oss.str();
                dbnames_.push_back(pt_.get<string>(cfg.c_str()));
            }
        }

        vector<string> getDbNames(){
            return dbnames_;
        }

        int getBloomBits(){
            return bloomBits_;
        }

        bool getSeekCompaction(){
            return seekCompaction_;
        }

        bool getCompression(){
            return compression_;
        }

        bool getDirectIO(){
            return directIO_;
        }

        int getDiskNum(){
            return diskNum_;
        }
    };
}

#endif //YCSB_C_LEVELDB_DB_H
