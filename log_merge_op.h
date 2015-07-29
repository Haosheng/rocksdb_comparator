#ifndef STORAGE_ROCKSDB_INCLUDE_LOGMERGEOPERATOR_H_
#define STORAGE_ROCKSDB_INCLUDE_LOGMERGEOPERATOR_H_

#include <cstdio>
#include <string>
#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"
#include "rocksdb/merge_operator.h"

namespace rocksdb{

class LogMergeOperator : public AssociativeMergeOperator {
public:
	virtual ~LogMergeOperator(){}
	virtual bool Merge(const Slice& key,
        const Slice* existing_value,
        const Slice& value,
        std::string* new_value,
        Logger* logger) const override {
		std::string existing = "0 0 0 0 0 0 0 0 0 0 0";
		if(existing_value){
			//if(!Deserialize(*existing_value, &existing)){
			//	Log(logger, "existing value corruption");
			//	existing = "0 0 0 0 0 0 0 0 0 0 0";
			//}
                     existing = (*existing_value).ToString();
		}

		std::string to_merge = "0 0 0 0 0 0 0 0 0 0 0";
		//if(!Deserialize(value, &to_merge)){
		//	Log(logger, "to_merge value corruption");
		//	to_merge = "0 0 0 0 0 0 0 0 0 0 0";
		//}
                if(&value){
                to_merge = value.ToString();
                }

		std::string merge_res;
		std::size_t pos1=0,pos2=0;
		std::string delim = " ";
		for(int i=0;i<11;i++){
			pos1 = existing.find(delim);
			pos2 = to_merge.find(delim);
			int exis = std::stoi(existing.substr(0,pos1));
			int to_mrg = std::stoi(to_merge.substr(0,pos2));
			int mrg_res = exis + to_mrg;
			merge_res += std::to_string(mrg_res)+" ";
			existing.erase(0,pos1 + delim.length());
			to_merge.erase(0,pos2 + delim.length());
		}
		merge_res.erase(merge_res.length()-1,1); //erase the last space that appended wrongly
		*new_value = merge_res;
		return true;
	}

	virtual const char* Name() const override{
		return "LogMergeOperator";
	}
};

} // namespace rocksdb
#endif
