//
// Created by wujy on 11/3/18.
//

#ifndef YCSB_C_LEVELDB_CONFIG_H
#define YCSB_C_LEVELDB_CONFIG_H


#include "leveldb/db.h"
#include "core/db.h"
#include <string>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/ini_parser.hpp>

using std::string;

namespace ycsbc {
    class ConfigLevelDB {
    private:
        boost::property_tree::ptree pt_;
        int bloomBits_;
        bool seekCompaction_;
        bool compression_;
        bool directIO_;
        int vlogThreads_;
        int sizeRatio_;
        size_t blockCache_;
        string vlogFilename_;
        size_t gcSize_;
        size_t gcAfter_;

    public:
        ConfigLevelDB() {
            boost::property_tree::ini_parser::read_ini("./configDir/leveldb_config.ini", pt_);
            bloomBits_ = pt_.get<int>("config.bloomBits");
            seekCompaction_ = pt_.get<bool>("config.seekCompaction");
            compression_ = pt_.get<bool>("config.compression");
            directIO_ = pt_.get<bool>("config.directIO");
            blockCache_ = pt_.get<size_t>("config.blockCache");
            vlogFilename_ = pt_.get<string>("vlog.vlogFilename");
            vlogThreads_ = pt_.get<int>("vlog.scanThreads");
            sizeRatio_ = pt_.get<int>("config.sizeRatio");
            gcSize_ = pt_.get<size_t>("vlog.gcSize");
            gcAfter_ = pt_.get<size_t>("vlog.gcAfter");
        }

        int getBloomBits() {
            return bloomBits_;
        }

        bool getSeekCompaction() {
            return seekCompaction_;
        }

        bool getCompression() {
            return compression_;
        }

        bool getDirectIO() {
            return directIO_;
        }

        int getVlogThreads() {
            return vlogThreads_;
        }

        string getVlogFilename() {
            return vlogFilename_;
        }

        size_t getBlockCache() {
            return blockCache_;
        }

        int getSizeRatio() {
            return sizeRatio_;
        }

        size_t getGcSize(){
            return gcSize_;
        }

        size_t getGcAfter(){
            return gcAfter_;
        }
    };
}

#endif //YCSB_C_LEVELDB_DB_H
