#include <iostream>
#include <string>
#include <fstream>
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/iterator.h"
#include "rocksdb/slice.h"
#include "rocksdb/DLR_key_comparator.h"
#include "rocksdb/logdata_op.h"

using namespace rocksdb;

#define ID_SIZE 10
#define KEYSIZE  (sizeof(unsigned int)+sizeof(unsigned long long)+ID_SIZE)
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

int main(int argc, char* argv[]){	
	std::string cp_str = argv[2];
	std::string ts1_str;
	std::string ts2_str;
	ts1_str = argv[3];
	ts2_str = argv[4];

	char head[KEYSIZE+1];
	char tail[KEYSIZE+1];

	GenKey(cp_str+"_"+ts1_str+"_          ", head);
	GenKey(cp_str+"_"+ts2_str+"_          ", tail);

	Slice hd = head;
	Slice tl = tail;

	DLRKeyComparator cmp;
	DB *db;
	Options options;
  	// Optimize RocksDB. This is the easiest way to get RocksDB to perform well
  	options.IncreaseParallelism();
  	options.OptimizeLevelStyleCompaction();
  	options.comparator = &cmp;
  	std::string DBpath = argv[1];
  	Status status = DB::Open(options, DBpath , &db);
  	if (!status.ok()) std::cerr << status.ToString() << std::endl;
  	assert(status.ok());

  	Iterator* it = db->NewIterator(ReadOptions());
  	 int count =0;
  	for(it->Seek(hd);
       it->Valid() /*&& cmp.Compare(it->key(),tl)<0*/;
       it->Next()) {
       	count++;    
  		}

  	std::cout<<"total found: "<<count<<"logs."<<std::endl;
  	return 0;


}