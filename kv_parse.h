#ifndef STORAGE_ROCKSDB_INCLUDE_KVPARSER_H_
#define STORAGE_ROCKSDB_INCLUDE_KVPARSER_H_

#include <string>
#include <cstdio>
#include <utility>

namespace rocksdb{
	class KVParser{
	public:
		~KVParser(){}
		std::pair<std::string, std::string> parse_line(std::string Line){
			std::string toParse = Line;
			std::string key, value;
			std::string delimiter = ":";
			std::size_t pos=0;
			pos = Line.find(delimiter);	
			key = toParse.substr(0,pos);
			value = toParse.substr(pos+1);
			std::pair<std::string,std::string> result;
			result = std::make_pair(key,value);
			return result;
		}
	};

}
#endif