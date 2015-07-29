#ifndef STORAGE_ROCKSDB_INCLUDE_DLRCOMPARATOR_
#define STORAGE_ROCKSDB_INCLUDE_DLRCOMPARATOR_

#include <cstring>
#include <string>
#include "rocksdb/db.h"
#include "rocksdb/comparator.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"

#define ID_SIZE 10
namespace rocksdb {
	class DLRKeyComparator : public Comparator {
	public:
		int Compare(const Slice& a, const Slice& b) const {
			unsigned int a1, a2;
			unsigned long long b1, b2;
			char *s1=new char[ID_SIZE+1];
			char *s2=new char[ID_SIZE+1];
			ParseKey(a, &a1, &b1, s1);
			ParseKey(b, &a2, &b2, s2);
			if (a1 < a2){
				delete s1;
				delete s2;
				return -1;
			} 
			if (a1 > a2){
				delete s1;
				delete s2;
				return +1;
			} 
			if (b1 < b2) {
				delete s1;
				delete s2;
				return -1;
			}
			if (b1 > b2) {
				delete s1;
				delete s2;
				return +1;
			}
			std::string str1=s1, str2=s2;
			if (str1 < str2) {
				delete s1;
				delete s2;
				return -1;
			}
			if (str1 > str2){
				delete s1;
				delete s2;
				return +1;
			} 
			delete s1;
			delete s2;
			return 0;
    	}

    	const char* Name() const { return "DLRKeyComparator"; }
    	void FindShortestSeparator(std::string*, const rocksdb::Slice&) const { }
    	void FindShortSuccessor(std::string*) const { }
	private:
		void ParseKey(const Slice& a, unsigned int* a1, unsigned long long* b1, char* s1) const{
			std::memcpy(a1,a.data(),sizeof(unsigned int));
			std::memcpy(b1,a.data()+sizeof(unsigned int),sizeof(unsigned long long));
			s1[ID_SIZE]=0;
			std::memcpy(s1,a.data()+sizeof(unsigned int)+sizeof(unsigned long long),ID_SIZE);
		}
	};
}

#endif
