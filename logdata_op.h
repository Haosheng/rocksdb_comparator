#ifndef STORAGE_ROCKSDB_INCLUDE_LOGDATAOPERATOR_H_
#define STORAGE_ROCKSDB_INCLUDE_LOGDADAOPERATOR_H_

#include <iostream>
#include <string>
#include <memory>
#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"
#include "rocksdb/log_merge_op.h"

#define ID_SIZE 10
#define KEYSIZE  (sizeof(unsigned int)+sizeof(unsigned long long)+ID_SIZE)
namespace rocksdb{
class LogDataOperator {
     public:
      LogDataOperator(std::shared_ptr<DB> db):db_(db),get_option_(),put_option_(),delete_option_(),merge_option_(){}

      // mapped to a RocksDB Put
      virtual void Set(const std::string& key, std::string value) {
        put_option_.sync = false;
        char *newkey=new char[KEYSIZE+1];
        GenKey(key,newkey);
        Slice sk = newkey, sv = value;
        db_->Put(put_option_, sk,  sv);
	delete newkey;
      }

      // mapped to RocksDB sync Put
      virtual void Sync_Set(const std::string& key, std::string value){
        put_option_.sync = true;
        char *newkey=new char[KEYSIZE+1];
	GenKey(key,newkey);
        Slice sk = newkey, sv = value;
        db_->Put(put_option_, sk,  sv);
	delete newkey;
      }

      // mapped to a RocksDB Delete
      virtual void Remove(const std::string& key) {
        Slice sk = key;
        db_->Delete(delete_option_, sk);
      }

      // mapped to a RocksDB Get
      virtual bool Get(const std::string& key, std::string *value) {
        std::string str;
        char *newkey=new char[KEYSIZE+1];
	GenKey(key,newkey);
        Slice sk = newkey;
        auto s = db_->Get(get_option_, sk,  &str);
        delete newkey;
	if (s.ok()) {
          *value = str;
          return true;
        } else {
          std::cerr<<s.ToString()<<std::endl;
          return false;
        }
      }

      // implemented as get -> modify -> set
      virtual void Add(const std::string& key, std::string value) {
        char *newkey=new char[KEYSIZE+1];
	GenKey(key,newkey);
        Slice sk = newkey, sv = value;
        db_->Merge(merge_option_, sk, sv);
	delete newkey;
      }
    private:
      std::shared_ptr<DB> db_;
      ReadOptions get_option_;
      WriteOptions put_option_, merge_option_, delete_option_;

      void GenKey(std::string line,char* key){
        std::string read = line;
        std::string delim = "_";
        size_t pos = 0;
        pos = read.find(delim);
        std::string cp_str = read.substr(0,pos);
        unsigned int cp = std::stoul(cp_str);
        memcpy(key, &cp, sizeof(unsigned int));
        read = read.substr(pos+1);
        pos = read.find(delim);
        std::string ts_str = read.substr(0,pos);
        //std::string dot=".";
        //size_t ms=ts_str.find(dot);
        //std::string sec=ts_str.substr(0,ms);
        //std::string msec=ts_str.substr(ms+1);
        //unsigned long long ts= (unsigned long long)stoi(sec)*1000+(unsigned long long)stoi(msec);
        unsigned long long ts = std::stoull(ts_str);
        memcpy(key+sizeof(unsigned int),&ts,sizeof(unsigned long long));
        read = read.substr(pos+1);
        char *id = new char[read.size()+1];
        id[read.size()]=0;
        memcpy(id,read.c_str(),read.size());
        memcpy(key+sizeof(unsigned int)+sizeof(unsigned long long), id, read.size()+1);
        delete id;
	}

    };
  }

  #endif
