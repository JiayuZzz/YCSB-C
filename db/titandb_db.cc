//
// Created by 吴加禹 on 2019-07-17.
//

#include "titandb_db.h"
#include <iostream>
#include "rocksdb/filter_policy.h"
#include "rocksdb/cache.h"
#include "leveldb_config.h"
#include "rocksdb/statistics.h"
#include "rocksdb/flush_block_policy.h"

using namespace std;

namespace ycsbc {
    TitanDB::TitanDB(const char *dbfilename) :noResult(0){
        //get leveldb config
        ConfigLevelDB config = ConfigLevelDB();
        int bloomBits = config.getBloomBits();
        size_t blockCache = config.getBlockCache();
        bool seekCompaction = config.getSeekCompaction();
        bool compression = config.getCompression();
        bool directIO = config.getDirectIO();
        size_t memtable = config.getMemtable();
        //set optionssc
        rocksdb::titandb::TitanOptions options;
        rocksdb::BlockBasedTableOptions bbto;
        options.create_if_missing = true;
        options.write_buffer_size = memtable;
	    options.max_background_gc = 1;
        options.blob_file_discardable_ratio = 0.3;
	    options.disable_background_gc = false;
        options.compaction_pri = rocksdb::kMinOverlappingRatio;
        options.max_bytes_for_level_base = memtable;
	    options.target_file_size_base = 32<<20;
        options.statistics = rocksdb::CreateDBStatistics();
        if(!compression)
            options.compression = rocksdb::kNoCompression;
        if(bloomBits>0) {
            bbto.filter_policy.reset(rocksdb::NewBloomFilterPolicy(bloomBits));
        }
        options.min_gc_batch_size = 8<<20;
        bbto.block_cache = rocksdb::NewLRUCache(blockCache);
        options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(bbto));
        options.blob_file_target_size = 16<<20;
        options.level_merge = config.getLevelMerge();
	    options.range_merge = config.getRangeMerge();
        if(options.level_merge)
	        options.level_compaction_dynamic_level_bytes = true;
        options.sep_before_flush = config.getSepBeforeFlush();
        if(config.getTiered()) options.compaction_style = rocksdb::kCompactionStyleUniversal;
        options.max_background_jobs = config.getNumThreads();
        options.disable_auto_compactions = config.getNoCompaction();
        options.mid_blob_size = config.getMidThresh();
        options.min_blob_size = config.getSmallThresh();
        // if(options.level_merge){
            // options.max_bytes_for_level_base = options.write_buffer_size*64;
	        // options.level0_file_num_compaction_trigger*=4;
            // options.level0_slowdown_writes_trigger*=4;
            // options.level0_stop_writes_trigger*=4;
        // }

        rocksdb::Status s = rocksdb::titandb::TitanDB::Open(options, dbfilename, &db_);
        if(!s.ok()){
            cerr<<"Can't open titandb "<<dbfilename<<endl;
            exit(0);
        }
        cout<<"\nbloom bits:"<<bloomBits<<"bits\ndirectIO:"<<(bool)directIO<<"\nseekCompaction:"<<(bool)seekCompaction<<endl;
    }

    int TitanDB::Read(const std::string &table, const std::string &key, const std::vector<std::string> *fields,
                      std::vector<KVPair> &result) {
        string value;
        rocksdb::Status s = db_->Get(rocksdb::ReadOptions(),key,&value);
        if(s.ok()) return DB::kOK;
        if(s.IsNotFound()){
            noResult++;
            cout<<noResult<<endl;
            return DB::kOK;
        }else{
            cerr<<"read error"<<s.ToString()<<endl;
            return DB::kOK;
        }
    }


    int TitanDB::Scan(const std::string &table, const std::string &key, int len, const std::vector<std::string> *fields,
                      std::vector<std::vector<KVPair>> &result) {
        auto it=db_->NewIterator(rocksdb::ReadOptions());
        it->Seek(key);
        std::string val;
        std::string k;
        for(int i=0;i<len&&it->Valid();i++){
            k = it->key().ToString();
            val = it->value().ToString();
            it->Next();
        }
        return DB::kOK;
    }

    int TitanDB::Insert(const std::string &table, const std::string &key,
                        std::vector<KVPair> &values){
        rocksdb::Status s;
        for(KVPair &p:values){
            s = db_->Put(rocksdb::WriteOptions(),key,p.second);
            if(!s.ok()){
                cerr<<"insert error\n";
                cerr<<s.ToString()<<endl;
                // exit(0);
            }
        }
        return DB::kOK;
    }

    int TitanDB::Update(const std::string &table, const std::string &key, std::vector<KVPair> &values) {
        return Insert(table,key,values);
    }

    int TitanDB::Delete(const std::string &table, const std::string &key) {
        vector<DB::KVPair> values;
        return Insert(table,key,values);
    }

    void TitanDB::printStats() {
        string stats;
        db_->GetProperty("rocksdb.stats",&stats);
        cout<<stats<<endl;
    }

    TitanDB::~TitanDB() {
        delete db_;
    }
}
