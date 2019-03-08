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
        int sizeRatio_;
        size_t writeBuffer_;
        size_t blockCache_;

    public:
        ConfigLevelDB() {
            boost::property_tree::ini_parser::read_ini("./configDir/leveldb_config.ini", pt_);
            bloomBits_ = pt_.get<int>("config.bloomBits");
            seekCompaction_ = pt_.get<bool>("config.seekCompaction");
            compression_ = pt_.get<bool>("config.compression");
            directIO_ = pt_.get<bool>("config.directIO");
            blockCache_ = pt_.get<size_t>("config.blockCache");
            sizeRatio_ = pt_.get<int>("config.sizeRatio");
            writeBuffer_ = pt_.get<size_t>("config.writeBuffer");
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

        size_t getBlockCache() {
            return blockCache_;
        }

        int getSizeRatio() {
            return sizeRatio_;
        }

        size_t getWriteBuffer(){
            return writeBuffer_;
        }
    };
}

#endif //YCSB_C_LEVELDB_DB_H
