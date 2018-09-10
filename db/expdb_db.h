//
// Created by wujy on 9/10/18.
//

#ifndef YCSB_C_EXPDB_DB_H
#define YCSB_C_EXPDB_DB_H


#include "leveldb/expdb.h"
#include "core/db.h"
#include <string>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/ini_parser.hpp>

using std::string;

namespace ycsbc {
    class LevelDBExp : public DB{
    public:
        LevelDBExp(const char *dbfilename);
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

        ~LevelDBExp();

    private:
        leveldb::ExpDB *db_;
        unsigned noResult;
    };
}


#endif //YCSB_C_EXPDB_DB_H
