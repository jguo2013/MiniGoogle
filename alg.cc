/**************************
   TinyGoogle project
   task.cc
   author: Jie Guo jig26@pitt.edu
   version: 1.0 /11/29/2013 
**************************/
#include <sys/types.h>
#include <ctype.h>
#include <sys/stat.h>
#include <stdio.h>
#include <dirent.h> 
#include <stdio.h> 
#include <sstream>
#include <cctype>
#include <algorithm>
#include "alg.h"
#include "request.h"
#include "global.h"
using namespace std;

Algo::Algo(){} 
   
Algo::Algo(Machine * _m)
{
	host_name = _m->GetHostName();	
	req = _m->GetTaskOp();
	
  switch(_m->GetTaskOp())
  {
   case INDEX_MAP:
   	    file   = *(_m->GetFileName()->begin());
   	    num    = _m->GetNum();
   	    offset = _m->GetOffset();
   break;
   case INDEX_REDUCE:
   	    key  = *_m->GetKeys(); 
   	    num    = _m->GetNum(); 
   	    offset = _m->GetOffset();   	      	    
   break;   	
   case SEARCH_REDUCE:
   	    num = _m->GetNum();
   	    key = *_m->GetKeys();    	
   break;   	
   default:;
  }
  reduce_index.clear();reduce_index.clear();sort_index.clear();
}

Algo::~Algo(){}
   
void *Algo::SearchWord(void *_t)
{
  arg_t *p = (arg_t *)_t; int id = (int)(p->id); int total = p->total; 
	FILE *fp = fopen(file.c_str(),"r");  
	//count for word
	int start_flag; char b[512]; int last_word;
	int _num    = slib::getlen(num,total,id);
	int _offset = slib::getoffset(offset,num,total,id); 

	if(_offset) {fseek(fp ,_offset-1, SEEK_SET ); start_flag=1; _num++;}
	else        {start_flag=0;}                   	
	string s; s.clear();
    		
  while(fgets(b, sizeof(b), fp))
  {	
    s.insert(s.size(),b); last_word=0;        
    //set the start of word search
    if(start_flag)
    {    
       if(!SetStartWord(&s,&_num)) break;
       start_flag = 0; 
    }
    while(!last_word && _num>0 && s.size())
    {
      string s1 = ExtractWord(&s,&last_word,1,&_num);
      if(s1.size()) {
      	pthread_mutex_lock(p->lock);
      	UpdateMapIndex(s1,1);
      	pthread_mutex_unlock(p->lock);
      }
    }    
    if(_num <=0){ s.clear(); break;}

  }
  
  if(s.size()) 
  	{
  	 string s1 = ExtractWord(&s,&last_word,0,&_num); 
     pthread_mutex_lock(p->lock);
     UpdateMapIndex(s1,1);
     pthread_mutex_unlock(p->lock);
    } 
  fclose(fp); 
}
  
int Algo::FlushMap()
{
	string fn = "./tmp/";
	fn.insert(fn.size(),host_name);
	fn.insert(fn.size(),"_index_map.txt");
	
	FILE *fp = fopen(fn.c_str(),"w");
	map<char, map<string,int> >::iterator it;
		
	for(it=map_index.begin(); it!=map_index.end();it++)
	{
		 map<string,int>::iterator itn;
	   for(itn=it->second.begin(); itn!=it->second.end();itn++)	
	   {
	   	 fprintf(fp,"%s %s %d\n",itn->first.c_str(),file.c_str(),itn->second);
	   }		
	}
	fclose(fp); 
}
 
void *Algo::ReduceIndex(void *_t)
{
  char b[128]; string f;  
  arg_t *p = (arg_t *)_t; int id = (int)(p->id); int total = p->total; 
	int _num    = slib::getlen(num,total,id);
	int _offset = slib::getoffset(offset,num,total,id); 

	for(deque<string>::iterator it=key.begin();it!=key.end();it++)//record host name
	{
	  //read one data from a file
	  string fn = "./tmp/"; 
    fn.insert(fn.size(),(*it).c_str());
	  fn.insert(fn.size(),"_index_map.txt");
	  FILE *fp = fopen(fn.c_str(),"r");	
		while(fgets(b, sizeof(b), fp))
		{			
		 string w; int count; char *pch;
		 pthread_mutex_lock(p->lock);			  
		 pch = strtok (b," ");      w = pch;
		 pch = strtok (NULL, " ");  f = pch;
		 pch = strtok (NULL, " ");  count = atoi(pch);
		 pthread_mutex_unlock(p->lock);	
		 UpdateReduceIndex(w,count,_num,_offset,p->lock); 	 			 
		}
		pthread_mutex_lock(p->lock);
		file = f; 
		pthread_mutex_unlock(p->lock);
		fclose(fp); 
	}
	Algo::FlushIndex(_offset,_num,id,p->lock);
}

int Algo::FlushIndex(int _o, int _n, int _id, pthread_mutex_t * _l)
{
  //check the inverted file already exist
  deque<map_t> s;  s.clear(); 
  map<string, int>::iterator it;
  for(it = reduce_index.begin();it != reduce_index.end();it++)
  {
  	s.clear();it->first; 
  	if(!CheckRange(it->first,_n,_o)) continue;
  	if(!CheckInvertIndex(it->first)) UpdateSortIndex(file, it->second,&s);
    else
    {
    	RetrieveInvertIndex(it->first,&s,_l);
    	UpdateInvertIndex(file,it->second,&s);
    }
    
    FlushInvertedIndex(it->first,&s,_id);
  }
}

void* Algo::SearchIndex(void * _t)
{
  arg_t *p = (arg_t *)_t; int id = (int)(p->id); int total = p->total; 
  deque<string> s = slib::getkey(&key, total, id);deque<string>::iterator it;
  for(it = s.begin();it != s.end();it++)
  { 
 	  transform((*it).begin(),(*it).end(),(*it).begin(),::tolower);
  	if(CheckInvertIndex(*it))
  	{
      RetrieveInvertIndex(*it,&sort_index,p->lock);     
    }  	  	
  } 		
}

int Algo::SetStartWord(string *_s, int * _n)		//determine the start letter
{
	int f = 0; 
	while(f != 1 && *_n > 0)
	{
		if((((*_s).at(0) >= 'a' && (*_s).at(0) <= 'z') ||
			  ((*_s).at(0) >= 'A' && (*_s).at(0) <= 'Z'))&&(*_s).size())
			 {(*_s).erase((*_s).begin()); (*_n)--;} 	//the character before offset: remove the first word         
	  else {f = 1;}
	} 
	return((*_n) > 0);
} 

string Algo::ExtractWord(string *_s, int * _f, int _c, int * _n)
{
  string s; s.clear(); int ready = 0, cpl = 0;
	while((*_s).size())
	{
		switch(ready)
		{
		   case 0: //not ready
		           if(((*_s).at(0) >= 'a' && (*_s).at(0) <= 'z')||
			            ((*_s).at(0) >= 'A' && (*_s).at(0) <= 'Z')) 
			             ready = 1;
			         else
	  	           {
	  	           	if((*_s).size()){(*_s).erase((*_s).begin());(*_n)--;}
	  	           }			         	  
			         break;
			 case 1: //start to extract word
		           if((((*_s).at(0) >= 'a' && (*_s).at(0) <= 'z') ||
			             ((*_s).at(0) >= 'A' && (*_s).at(0) <= 'Z'))&&(*_s).size()) 
	  	           {
	  	           	 char c = (*_s).at(0); c = tolower(c);(*_n)--;  	  
	  	             s.push_back(c); (*_s).erase((*_s).begin());
	  	           }
			         else
			         	 { cpl = 1;}
			         break;			 	       
			 default:;	         			   	       
		}
		
		if(cpl==1)break;
	}
	//the word is the last word in the stream, but it may not be incomplete
	if(cpl == 0 && _c == 1) {(*_n)+=s.size(); (*_s).insert(0,s);s.clear(); *_f=1;}
	return(s);		
}
    	
int Algo::UpdateMapIndex(string _s, int _c)
{
	char c = _s.at(0);
	map<char, map<string,int> >::iterator it = map_index.find(c);
	if(it == map_index.end())
	{
	  map<string,int> m; m[_s]=_c;
	  map_index.insert(pair<char, map<string,int> >( c,m));
	}
  else
  {
    InsertMapEntry(_s, _c, &(it->second));    
  }

}

int Algo::InsertMapEntry(string _s, int _c, map<string,int> *_m)
{
    map<string,int>::iterator it = (*_m).find(_s);
	  if(it == (*_m).end())
	  {
	    (*_m).insert(pair<string, int>(_s,_c));
	  }
	  else
	  {
	  	it->second += _c;
	  } 
}

int Algo::CheckRange(string _w,int _n,int _o)
{
	char c = _w.at(0); 
	char up, low;
	up='a'+_n+_o; 	low='a'+_o; 
	//check the string is in current sort range  
	if(c>=low && c<up) return(1);
	else return(0);
}
    
int Algo::UpdateReduceIndex(string _w, int _c, int _n, int _o,pthread_mutex_t * _l)
{
	if(Algo::CheckRange(_w,_n,_o))
	{ 
		pthread_mutex_lock(_l);
		InsertMapEntry(_w,_c,&reduce_index);
		pthread_mutex_unlock(_l);
	}	
}

int Algo::RetrieveInvertIndex(string _s, deque<map_t> *_i, pthread_mutex_t * _l)
{
		char b[64];
		string fn = GenIndexFile(_s);
		FILE *fp = fopen(fn.c_str(),"r");
		if(!fp)
		{
      cerr << "Index file is not found"<< endl;	 
		}
		else
		{
		  while(fgets(b, sizeof(b), fp))
		  {
		   string f; int count; char *pch; 
		   pthread_mutex_lock(_l);
		   pch = strtok(b," ");       f = pch;
		   pch = strtok (NULL, " \n");count = atoi(pch);
  	   UpdateSortIndex(f, count,_i); 
  	   pthread_mutex_unlock(_l);   	     
		  }
		}
		fclose(fp);
} 

int Algo::UpdateSortIndex(string _f, int _c, deque<map_t> *_s)
{
   map_t map_entry;
   map_entry.word = _f;
   map_entry.count = _c;
   (*_s).push_back(map_entry);	
}

int Algo::UpdateInvertIndex(string _f, int _c, deque<map_t> *_s)
{
	 deque<map_t>::iterator it;
	 for(it=(*_s).begin();it!=(*_s).end();it++)
   {
   	 if((*it).count<_c) break;
   }
   map_t map_entry;
   map_entry.word = _f;
   map_entry.count = _c; 
   (*_s).insert(it,map_entry); 
}

int Algo::FlushInvertedIndex(string _s, deque<map_t> *_i, int _id)
{
	  string fn = GenIndexFile(_s);	  
	  FILE *fp = fopen(fn.c_str(),"w");
	  deque<map_t>::iterator it;
	  for(it=(*_i).begin();it!=(*_i).end();it++)
    {
     fprintf(fp, "%s %d\n",(*it).word.c_str(),(*it).count);
    }
    fclose(fp);
}   
    
int Algo::CheckInvertIndex(string _s)
{
  string fn = GenIndexDir(_s);
  DIR *d; int found = 0;
  struct dirent *dir;
  d = opendir(fn.c_str());
  if(d)		
  {
    while((dir = readdir(d)) != NULL)
    {
      string s = _s; s.insert(s.size(),".txt");
      if(strcmp(dir->d_name,s.c_str()) == 0)
      {found = 1;break;}
    }
    closedir(d);
  }
  else												
  { mkdir(fn.c_str(),0777);}	
  return(found);	
}

string Algo::GenIndexDir(string _s)
{
  char c = _s.at(0);
	string s = "./index/";
	s.push_back(c);
	return(s);
}

string Algo::GenIndexFile(string _s)
{
	string s = GenIndexDir(_s); s.push_back('/');
	s.insert(s.size(),_s.c_str());
  s.insert(s.size(),".txt");
  return(s);	
}
  
int	Algo::UpdateSearchIndex()	//update reduce_index
{
	deque<map_t>::iterator it;
	for(it=sort_index.begin();it!=sort_index.end();it++)
  {
     InsertMapEntry((*it).word,(*it).count,&reduce_index);    
  }
}

deque<string> * Algo::GetFilebyCount()
{
	deque<string>* queue = new deque<string>;
  for(int i=0;i<3;i++)
  { 	
  	int cmax = 0;
  	map<string, int>::iterator it;
  	map<string, int>::iterator itn = reduce_index.end();	
    for(it=reduce_index.begin();it!=reduce_index.end();it++)
    {
  	   if(it->second > cmax)
  	   	{cmax = it->second; itn = it;}   
    }
    if(itn != reduce_index.end())
    {
       
       (*queue).push_back(itn->first); 
       (*queue).push_back(slib::_itoa(itn->second));
        reduce_index.erase(itn);
    } 
	}
	return(queue);
}

ostream & Algo::Print(ostream &os) const
{
   os << "Algo::  file: "<<file<<", offset: "<<offset<<", num: "<< num<<", host_name: " <<host_name <<" \n"
      << "        key:  "<< endl;
        
   for(deque<string>::const_iterator it=key.begin();it!=key.end();it++)
   { 
      os <<*it<<"-> ";   	  	      
   }  os <<"\n"; 

   os << "        sort_index:  " << endl;    
   for(deque<map_t>::const_iterator it = sort_index.begin(); it != sort_index.end();it++) 
   {
      os << "(w: " << (*it).word <<" c: " << (*it).count 
         << ") -> ";   	  	            	  	      
   }  os << "\n"; 

   os << "        reduce_index: " << endl;       
   for(map<string, int>::const_iterator it = reduce_index.begin(); it != reduce_index.end();it++) 
   {
      os << "(w: " << it->first <<" c: " << it->second 
         << ") -> ";   	  	            	  	      
   }  os << "\n";  

   os << "        map_index:    " << endl;          
   for(map<char, map<string,int> >::const_iterator it = map_index.begin(); it != map_index.end();it++) 
   {
     os << "                  " << it->first << "\n                  "<< endl;  
     for(map<string, int>::const_iterator i = it->second.begin(); i != it->second.end();i++) 
     {      
       os << "(w: " << i->first <<" c: " << i->second 
          << ") -> "; 
     } os << "\n";
   }       
   return os;
}
