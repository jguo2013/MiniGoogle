/**************************
   TinyGoogle project
   algo.h
   author: Jie Guo jig26@pitt.edu
   version: 1.0 /11/26/2013 
**************************/
#ifndef _algo
#define _algo

#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <deque>
#include <map>
#include <iostream>
#include <fstream>
#include <string>
#include <pthread.h>
#include "machine.h"
#include "request.h"

typedef struct map_index_entry
{
  string word;	 
  unsigned count;
}map_t;

typedef struct argument
{
    long id; int total;
    pthread_mutex_t * lock;
}arg_t;

class Algo {
    
protected:
	 TaskOp  req;
   string  file;
   int     offset;
   int     num;
   string  host_name;   
   deque<string> key;   
   deque<map_t> sort_index;
   map<string, int> reduce_index;
   map<char, map<string,int> >  map_index;  
     
public:        
   Algo();     
   Algo(Machine * _m); 
   ~Algo();

    string ExtractWord(string *_s, int * _f, int _c, int * _n);
    string GenIndexDir(string _s);
    string GenIndexFile(string _s); 
           
    int RetrieveInvertIndex(string _s, deque<map_t> *_i, pthread_mutex_t * _l);    
    int SetStartWord(string *_s, int * _n); 
    int LastWord(string *_s);
    int CheckInvertIndex(string _s);	
        
    int UpdateMapIndex(string _s, int _c);
		int UpdateReduceIndex(string _w, int _c, int _n, int _o, pthread_mutex_t * _l);
    int UpdateSortIndex(string _f, int _c, deque<map_t> *_s);
    int UpdateInvertIndex(string _f, int _c, deque<map_t> *_s);
    
    int FlushInvertedIndex(string _s, deque<map_t> *_i, int _id);
    int	UpdateSearchIndex();
    int InsertMapEntry(string _s, int _c, map<string,int> *_m);
    int CheckRange(string _w,int _n,int _o);
    	
    int FlushMap();
    int FlushIndex(int _o, int _n, int _id, pthread_mutex_t * _l);   

    deque<string> * SearchIndex();  
    deque<string> * GetFilebyCount();   

    virtual void *SearchWord(void *_t); 
    virtual void *ReduceIndex(void *_t);
    virtual void *SearchIndex(void *_t);
                       
    ostream & Print(ostream &os) const ;
};
inline ostream & operator<<(ostream &os, const Algo & _c) { return _c.Print(os);}

#endif
