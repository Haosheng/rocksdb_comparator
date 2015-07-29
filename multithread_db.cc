#include <iostream>
#include <string>
#include <memory>
#include <fstream>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/tester_dlr.h"
// #include "rocksdb/DLR_key_comparator.h"

#include <boost/thread.hpp>

using namespace rocksdb;

class Writer{
public:
	//default constructor
	Writer() {}

	void start_write(std::shared_ptr<DB> dbp, std::string filepath){
		m_Thread = boost::thread(&Writer::write2db, this, dbp, filepath);
	}

	void join(){
		m_Thread.join();
	}

	bool write2db(std::shared_ptr<DB> dbp, std::string filepath ){
		Tester t_load(dbp);
		return t_load.Random_Write_Test(filepath);
	}
private:
	boost::thread m_Thread;
};

class Reader{
public:
	//default constructor
	Reader() {}

	void start_read(std::shared_ptr<DB> dbp, std::string filepath){
			m_Thread = boost::thread(&Reader::read_from_db, this, dbp, filepath);	
	}

	void join(){
		m_Thread.join();
	}

	bool read_from_db(std::shared_ptr<DB> dbp, std::string filepath){
		Tester t_read(dbp);
		std::string logpath="";
		return t_read.Random_Read_Test(filepath,logpath);
	}
private:
	boost::thread m_Thread;
};

int main(int argc, char *argv[])
{
	/*** Initialize the number of threads by user input ***/
	int nWrite = 0, nRead = 0;
	std::cout << "Please input number of write threads: ";
	std::cin >> nWrite;
	while(nWrite < 1){
		std::cout << std::endl;
		std::cout << "Invalid number of write threads! Please input again: ";
		std::cin >> nWrite;
	}
	std::cout << std::endl;
	std::cout << "Please input number of read threads: ";
	std::cin >> nRead;
	while(nRead < 1){
		std::cout << std::endl;
		std::cout << "Invalid number of read threads! Please input again: ";
		std::cin >> nRead;
	}
	std::cout << std::endl;

	/*** Construct the Writers and Readers ***/
	Writer w[nWrite];
	Reader r[nRead];


	/*** Open a database ***/
	//DLRKeyComparator cmp;
	DB* db;
	Options options;
  	options.IncreaseParallelism();
  	options.OptimizeLevelStyleCompaction();
  	options.create_if_missing = true;
  	//options.comparator = &cmp;
  	std::string DBpath = argv[1];
  	Status status = DB::Open(options, DBpath , &db);
  	assert(status.ok());
  	std::shared_ptr<DB> dbp(db);

  	/*** For now, use 2 files to store the paths of data files to write and read data from ***/
  	std::string w_origin_path = argv[2];
  	std::string r_origin_path = argv[3];
  	std::ifstream write_origin(w_origin_path);
  	std::ifstream read_origin(r_origin_path);
  	if(write_origin.is_open()){
  		for(int i=0; i<nWrite; i++){
  			std::string f_path;
  			if(getline(write_origin,f_path)){
  				w[i].start_write(dbp,f_path);
  			}
  			else{
  				std::cerr << "Fail to get a file path from write source."<<std::endl;
  				return -1;
  			}
  		}
  	}
  	else{
  		std::cerr << "Fail to open write source."<<std::endl;
  		return -1;
  	}

  	/*** For now do not write read log ***/
  	if(read_origin.is_open()){
  		for(int i=0; i<nRead; i++){
  			std::string f_path;
  			if(getline(read_origin,f_path)){
  				r[i].start_read(dbp,f_path);
  			}
  			else{
  				std::cerr << "Fail to get a file path from read source."<<std::endl;
  				return -1;
  			}
  		}
  	}
  	else{
  		std::cerr << "Fail to open read source."<<std::endl;
  		return -1;
  	}

  	write_origin.close();
  	read_origin.close();

  	/*** wait for all processes to join ***/
  	for(int i=0; i<nWrite; i++){
  		w[i].join();
  		std::cout<<"Write Thread: "<<i<<"finished"<<std::endl;
  	}

  	for(int i=0; i<nRead;i++){
  		r[i].join();
  		std::cout<<"Read Thread: "<<i<<"finished"<<std::endl;
  	}

  	std::cout<<"Finish all work."<<std::endl;
  	return 0;
  	
}