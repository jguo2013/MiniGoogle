/**************************
   TinyGoogle project
   machine.cc
   author: Jie Guo 
   version: 1.0 /11/26/2013 
**************************/
#include <netdb.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/utsname.h>
#include <errno.h>
#include <stdio.h>
#include <iostream>
#include <stdlib.h>
#include <fstream>
#include <stdio.h>
#include <string.h>
#include <string>
#include "machine.h"
	                      
Machine::Machine()
{
  id = 1; port = 0;    reg = false;	 
  mstate.state = idle; mstate.op = NOOP;
  mstate.num = 0;      mstate.offset= 0;	
  mstate.worker_num = 0;	
}

Machine::~Machine(){}
   
Machine::Machine(const Machine &_m)
{
  id   = _m.id; port = _m.port;   
  ip   = _m.ip; type = _m.type;
  host_name = _m.host_name; reg = false;
  mstate.state = _m.mstate.state; 
  mstate.keywords = _m.mstate.keywords;
  mstate.file  = _m.mstate.file;  
  mstate.op    = _m.mstate.op;
  mstate.num   = _m.mstate.num;
  mstate.offset= _m.mstate.offset;
  mstate.worker_num = 0;	
}
         
void Machine::SetIpAddr(string _ip)
{
	ip = _ip;
}

void Machine::SetPortNum(unsigned _port)
{
  port = _port;
}

void Machine::SetTaskOp(TaskOp _op)
{
	mstate.op = _op;
} 
void Machine::SetNum(unsigned _n)
{
	mstate.num = _n;
}

void Machine::SetOffset(unsigned _n)
{
	mstate.offset = _n;
}

void Machine::SetWorkerNum(int _n)
{
   mstate.worker_num = _n;
}

void Machine::SetData(Request _q)
{
   mstate.num      = _q.GetReqNum();
   mstate.offset   = _q.GetReqOffset();

   if(_q.GetReqType()!=SEARCH_REDUCE_REPLY && 
      _q.GetReqType()!=INDEX_MAP_REPLY && _q.GetReqType()!=INDEX_REDUCE_REPLY)
     mstate.keywords = _q.GetReqKey(); 
   if(_q.GetReqType()==INDEX_MAP_REQ || _q.GetReqType()==INDEX_REQ)
     mstate.file     = _q.GetReqFile();    
        
   switch(_q.GetReqType())
	  {
	  	case INDEX_REQ :       mstate.op = INDEX;	  break;	
	  	case SEARCH_REQ:       mstate.op = SEARCH;   break;	
	  	case SEARCH_REDUCE_REQ:mstate.op = SEARCH_REDUCE;break;	  		  		   	
	  	case INDEX_MAP_REQ:    mstate.op = INDEX_MAP;    break;  		
  	  case INDEX_REDUCE_REQ: mstate.op = INDEX_REDUCE; break;  	     		      	  	  	  	 	  		  	             	        	  	
	  	default:;
	  }   
}

void Machine::SetMachineState(MState _s)
{
   if(mstate.state != inproc && _s == cpl)
   {
       cerr << "Fatal Error: previous machine state is NOT correct in SetMachineState() !!" << endl;
       exit(-1);
   }
	mstate.state = _s;
}  

unsigned Machine::GetPortNum()
{
	return(port);
}  

unsigned Machine::GetNum()
{
	return(mstate.num);
}

unsigned Machine::GetOffset()
{
	return(mstate.offset);
}   

string Machine::GetIpAddr()
{
	return(ip);
}

int Machine::GetId()
{
	return(id);
}
             
Mtype Machine::GetMachineType()
{
	return(type);
} 
   
int Machine::GetWorkerNum()
{
	return(mstate.worker_num);
}
      
MState Machine::GetMachineState()
{
	return(mstate.state);
}

void Machine::SetReg(bool _f)
{
	reg = _f;
}    
 
bool Machine::GetReg()
{
	return(reg);
}    
  
Machine Machine::GetNameServer()
{
	Machine m; 	char b[128];
	FILE * fp = fopen("./WK_FILE.TXT","r");
  if(fp == NULL){cerr << "Getting name server info fails\n" << m << endl; exit(-1);}
  int   len = fread(b,1,128-1,fp); fclose(fp);
  char *ipt = strtok(b,":"); 
  char *p   = strtok (NULL, " ,\n."); 
  unsigned po = atoi(p);
  
  struct hostent *he;
  struct in_addr ipv4addr;
  
  inet_pton(AF_INET, ipt, &ipv4addr);
  he = gethostbyaddr(&ipv4addr, sizeof(ipv4addr), AF_INET);
  m.SetHostName(he->h_name); m.SetPortNum(po); 
  m.SetIpAddr(ipt); m.SetId(0);
  
  cerr << "Name Server info is as follows:\n" << m << endl;
	return(m);	
}  

const Machine & Machine::operator=(const Machine &rhs)      
{          
  id   = rhs.id;port = rhs.port;   
  ip   = rhs.ip;type = rhs.type;
  host_name = rhs.host_name;
  mstate.state = rhs.mstate.state; 
  mstate.keywords = rhs.mstate.keywords;
  mstate.file  = rhs.mstate.file;  
  mstate.op    = rhs.mstate.op;
  mstate.num   = rhs.mstate.num;
  mstate.offset= rhs.mstate.offset; 
  mstate.worker_num = rhs.mstate.worker_num;             
	return(*this);
}

void Machine::GetLocalhostInfo()
{
  struct utsname  lh;
  struct hostent  *hp;
  struct in_addr  **pptr;
    
  uname(&lh); 
  host_name = lh.nodename;
  hp   = gethostbyname(lh.nodename);
  if (hp == NULL)
  {
    cerr << "Fatal Error: gethostbyname failed in GetLocalhostInfo()!"<< endl;
    exit(-1);
  }  
  pptr = (struct in_addr **)hp->h_addr_list;  
  ip   = inet_ntoa(**(pptr));

}

const char * Machine::GetHostName()
{
  return(host_name.c_str());
}

void Machine::SetHostName(string _s)
{
	host_name = _s;  	
} 

TaskOp Machine::GetTaskOp()
{
	return(mstate.op);
}

deque<string> * Machine::GetKeys()
{
	return(&(mstate.keywords));
} 

deque<string> * Machine::GetFileName()
{
	return(&(mstate.file));
}

void Machine::SetKeyword(deque<string> _q)
{
	mstate.keywords = _q;
} 

void Machine::SetMachineType(Mtype _t)
{
	type = _t;
}

void Machine::SetId(int _id)
{
	id = _id;
}

ostream & Machine::Print(ostream &os) const
{
	 deque<string>::const_iterator it;
   os << "Machine(type="<<type<<", id="<<id<<", ip="<<ip<<", port="<< port<<", host_name=" <<host_name <<" \n"
      << "        WorkState(op="<<mstate.op<<", state="<<mstate.state<<", worker_num=" << mstate.worker_num 
      << ",num="<<mstate.num<<", offset="<<mstate.offset<<" )"<<" \n";
   
   os << "        file(";    
   for(it = (mstate.file).begin(); it != (mstate.file).end();it++)
   { 
      os <<(*it)<<", ";   	  	      
   }  os <<" )" << " \n"; 

   os << "        keywords(";    
   for(it = mstate.keywords.begin(); it != mstate.keywords.end();it++) 
   {
      os <<(*it)<<", ";   	  	      
   }  os <<" ))" << " \n";  
   return os;
}
 