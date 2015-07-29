#ifndef STORAGE_ROCKSDB_INCLUDE_TESTER_H_
#define STORAGE_ROCKSDB_INCLUDE_TESTER_H_

#include <string>
#include <fstream>
#include <utility>
#include "rocksdb/logdata_op.h"
#include "rocksdb/kv_parse.h"
#include "rocksdb/write_batch.h"

namespace rocksdb{

class Tester {
	public:
		Tester(std::shared_ptr<DB> db):log_op_(db),kvp_(),db_(db) {}

		bool Random_Write_Test(std::string filepath){
			std::ifstream log_file(filepath);
			if(log_file.is_open()){
				std::pair<std::string,std::string> k_v;
				std::string read_line;
				while(std::getline(log_file,read_line)){
  	    			k_v=kvp_.parse_line(read_line);
  	    			log_op_.Set(k_v.first,k_v.second);
  	    		}
  	    		log_file.close();
  	    		return true;
			}
			else{
				return false;
			}
		}

		bool Random_Sync_Write_Test(std::string filepath){
			std::ifstream log_file(filepath);
			if(log_file.is_open()){
				std::pair<std::string,std::string> k_v;
				std::string read_line;				
				while(std::getline(log_file,read_line)){
  	    			k_v=kvp_.parse_line(read_line);
  	    			log_op_.Sync_Set(k_v.first,k_v.second);
  	    		}
  	    		log_file.close();
  	    		return true;
			}
			else{
				return false;
			}
		}

		bool Random_Batch_Write_Test(std::string filepath){
			std::ifstream log_file(filepath);
			if(log_file.is_open()){
				std::pair<std::string,std::string> k_v;
				std::string read_line;
				WriteBatch batch;
				while(std::getline(log_file,read_line)){
  	    			k_v=kvp_.parse_line(read_line);
  	    			batch.Put(k_v.first,k_v.second);
  	    		}
  	    		db_->Write(WriteOptions(),&batch);
  	    		log_file.close();
  	    		return true;
			}
			else{
				return false;
			}
		}

		bool Random_Sync_Batch_Write_Test(std::string filepath){
			std::ifstream log_file(filepath);
			if(log_file.is_open()){
				std::pair<std::string,std::string> k_v;
				std::string read_line;
				WriteBatch batch;
				WriteOptions wop;
				wop.sync=true;
				while(std::getline(log_file,read_line)){
  	    			k_v=kvp_.parse_line(read_line);
  	    			batch.Put(k_v.first,k_v.second);
  	    		}
  	    		db_->Write(wop,&batch);
  	    		log_file.close();
  	    		return true;
			}
			else{
				return false;
			}
		}


		bool Random_Read_Test(std::string filepath, std::string logpath, bool writelog = false){
			std::ifstream data_file(filepath);
			std::ofstream log_file(logpath);
			if(data_file.is_open()){
				std::pair<std::string,std::string> k_v;
				std::string read_line,get_val;
				while(std::getline(data_file,read_line)){
  	    			k_v=kvp_.parse_line(read_line);
  	    			log_op_.Get(k_v.first,&get_val);
  	    			if(writelog){
  	    				log_file <<k_v.first<<": "<<get_val<<std::endl;
  	    				}
  	    			}
  	    		if(writelog) log_file.close();
  	    		data_file.close();
  	    		return true;
			}
			else{
				return false;
			}
		}

		bool Random_Update_Test(std::string filepath){
			return Random_Write_Test(filepath);
		}

		bool Random_Merge_Test(std::string filepath){
			std::ifstream log_file(filepath);
			if(log_file.is_open()){
				std::pair<std::string,std::string> k_v;
				std::string read_line;
				while(std::getline(log_file,read_line)){
  	    			k_v=kvp_.parse_line(read_line);
  	    			log_op_.Add(k_v.first,k_v.second);
  	    		}
  	    		log_file.close();
  	    		return true;
			}
			else{
				return false;
			}
		}
	private:
		std::shared_ptr<DB> db_;
		LogDataOperator log_op_;
		KVParser kvp_;
};

} //end namespace
#endif