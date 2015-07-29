#include <iostream>
#include <fstream>
#include <string>
#include <memory>
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/tester_dlr.h"
#include "rocksdb/write_batch.h"
#include "rocksdb/DLR_key_comparator.h"

using namespace rocksdb;

static std::string kDBPath = "";

int main(int argc, char* argv[]){
	std::string operation=argv[1];
  DLRKeyComparator cmp;
	if(operation=="load"){
		DB* db;
		//std::shared_ptr<DB> db(std::make_shared<DB>());
  		Options options;
  		// Optimize RocksDB. This is the easiest way to get RocksDB to perform well
  		options.IncreaseParallelism();
  		options.OptimizeLevelStyleCompaction();
      options.comparator = &cmp;
  		// create the DB if it's not already present
  		options.create_if_missing = true;
  		if(argv[2] && argv[3]){
  			std::string DBname=argv[2];
  			std::string filepath=argv[3];
  			Status s = DB::Open(options, kDBPath+DBname, &db);
  			assert(s.ok());
  			std::shared_ptr<DB> dbp(db);
  			Tester t_load(dbp);
  			if(t_load.Random_Write_Test(filepath)){
  				std::cout<<"loaded data successfully"<<std::endl;
  			}
  			else{
  				std::cerr<<"load data failed: cannot open data file"<<std::endl;
  				exit (2);
  			}
  		}
  		else{
  			std::cerr<<"Please input database name and data file path."<<std::endl;
  			exit (1); 
  		}
	}

  else if(operation=="sync_load"){
    DB* db;
    //std::shared_ptr<DB> db(std::make_shared<DB>());
      Options options;
      // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
      options.IncreaseParallelism();
      options.OptimizeLevelStyleCompaction();
      options.comparator = &cmp;
      // create the DB if it's not already present
      options.create_if_missing = true;
      if(argv[2] && argv[3]){
        std::string DBname=argv[2];
        std::string filepath=argv[3];
        Status s = DB::Open(options, kDBPath+DBname, &db);
        assert(s.ok());
        std::shared_ptr<DB> dbp(db);
        Tester t_load(dbp);
        if(t_load.Random_Sync_Write_Test(filepath)){
          std::cout<<"loaded data successfully"<<std::endl;
        }
        else{
          std::cerr<<"load data failed: cannot open data file"<<std::endl;
          exit (2);
        }
      }
      else{
        std::cerr<<"Please input database name and data file path."<<std::endl;
        exit (1); 
      }
  }

  else if(operation=="batch_load"){
    DB* db;
    //std::shared_ptr<DB> db(std::make_shared<DB>());
      Options options;
      // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
      options.IncreaseParallelism();
      options.OptimizeLevelStyleCompaction();
      options.comparator = &cmp;
      // create the DB if it's not already present
      options.create_if_missing = true;
      if(argv[2] && argv[3]){
        std::string DBname=argv[2];
        std::string filepath=argv[3];
        Status s = DB::Open(options, kDBPath+DBname, &db);
        assert(s.ok());
        std::shared_ptr<DB> dbp(db);
        Tester t_load(dbp);
        if(t_load.Random_Batch_Write_Test(filepath)){
          std::cout<<"loaded data successfully"<<std::endl;
        }
        else{
          std::cerr<<"load data failed: cannot open data file"<<std::endl;
          exit (2);
        }
      }
      else{
        std::cerr<<"Please input database name and data file path."<<std::endl;
        exit (1); 
      }
  }


  else if(operation=="sync_batch_load"){
    DB* db;
    //std::shared_ptr<DB> db(std::make_shared<DB>());
      Options options;
      // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
      options.IncreaseParallelism();
      options.OptimizeLevelStyleCompaction();
      options.comparator = &cmp;
      // create the DB if it's not already present
      options.create_if_missing = true;
      if(argv[2] && argv[3]){
        std::string DBname=argv[2];
        std::string filepath=argv[3];
        Status s = DB::Open(options, kDBPath+DBname, &db);
        assert(s.ok());
        std::shared_ptr<DB> dbp(db);
        Tester t_load(dbp);
        if(t_load.Random_Sync_Batch_Write_Test(filepath)){
          std::cout<<"loaded data successfully"<<std::endl;
        }
        else{
          std::cerr<<"load data failed: cannot open data file"<<std::endl;
          exit (2);
        }
      }
      else{
        std::cerr<<"Please input database name and data file path."<<std::endl;
        exit (1); 
      }
  }

	else if(operation=="update"){
		//std::shared_ptr<DB> db(std::make_shared<DB>());
		DB* db;
  		Options options;
  		// Optimize RocksDB. This is the easiest way to get RocksDB to perform well
  		options.IncreaseParallelism();
  		options.OptimizeLevelStyleCompaction();
      options.comparator = &cmp;
  		if(argv[2] && argv[3]){
  			std::string DBname=argv[2];
  			std::string filepath=argv[3];
  			Status s = DB::Open(options, kDBPath+DBname, &db);
  			assert(s.ok());
  			std::shared_ptr<DB> dbp(db);
  			Tester t_up(dbp);
  			if(t_up.Random_Update_Test(filepath)){
  				std::cout<<"updated data successfully"<<std::endl;
  			}
  			else{
  				std::cerr<<"update data failed: cannot open data file"<<std::endl;
  				exit (2);
  			}
  		}
  		else{
  			std::cerr<<"Please input database name and data file path."<<std::endl;
  			exit (1); 
  		}
	}

	else if(operation=="merge"){
		//std::shared_ptr<DB> db(std::make_shared<DB>())
		DB* db;
  		Options options;
  		// Optimize RocksDB. This is the easiest way to get RocksDB to perform well
  		options.IncreaseParallelism();
  		options.OptimizeLevelStyleCompaction();
      options.comparator = &cmp;
  		//std::shared_ptr<LogMergeOperator> lmo;
  		options.merge_operator.reset(new LogMergeOperator);
  		// create the DB if it's not already present
  		options.create_if_missing = true;
  		if(argv[2] && argv[3]){
  			std::string DBname=argv[2];
  			std::string filepath=argv[3];
  			Status s = DB::Open(options, kDBPath+DBname, &db);
  			assert(s.ok());
  			std::shared_ptr<DB> dbp(db);
  			Tester t_merge(dbp);
  			if(t_merge.Random_Merge_Test(filepath)){
  				std::cout<<"merge data successfully"<<std::endl;
  			}
  			else{
  				std::cerr<<"merge data failed: cannot open data file"<<std::endl;
  				exit (2);
  			}
  		}
  		else{
  			std::cerr<<"Please input database name and data file path."<<std::endl;
  			exit (1); 
  		}
	}

	else if(operation=="get"){
		//std::shared_ptr<DB> db(std::make_shared<DB>());
		DB* db;
  		Options options;
  		// Optimize RocksDB. This is the easiest way to get RocksDB to perform well
  		options.IncreaseParallelism();
  		options.OptimizeLevelStyleCompaction();
      options.comparator = &cmp;
  		options.merge_operator.reset(new LogMergeOperator);
  		if(argv[2] && argv[3] && argv[4]){
  			std::string DBname=argv[2];
  			std::string filepath=argv[3];
			std::string writelog=argv[4];
  			Status s = DB::Open(options, kDBPath+DBname, &db);
  			assert(s.ok());
  			std::shared_ptr<DB> dbp(db);
  			Tester t_read(dbp);
  			if(writelog=="0"){
  				if(t_read.Random_Read_Test(filepath, "")){
  					std::cout<<"got data successfully"<<std::endl;
  				}
  				else{
  					std::cerr<<"get data failed: cannot open data file"<<std::endl;
  					exit (2);
  				}
  			}
  			else if(writelog=="1"){
  				std::string logpath=argv[5];
  				if(t_read.Random_Read_Test(filepath, logpath, true)){
  					std::cout<<"got data successfully"<<std::endl;
  				}
  				else{
  					std::cerr<<"get data failed: cannot open data file"<<std::endl;
  					exit (2);
  				}
  			}
  			else {
  				std::cerr<<"Invalid input, please specify 0 for don't write log, 1 for write log"<<std::endl;
  				exit (3);
  			}
  		}

  		else{
  			std::cerr<<"Please input database name, data file path, write/not write log, log file path."<<std::endl;
  			exit (1); 
  		}
	}
	else{
		std::cerr<<"Invalid operation type, please input load/update/merge/get."<<std::endl;
	}
	return 0;
}
