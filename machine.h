#ifndef _machine
#define _machine

#include <iostream>
#include <new>
#include <string>
#include <deque>
#include "request.h"
using namespace std;

enum TaskOp{LISTEN, INDEX, SEARCH, INDEX_MAP, INDEX_REDUCE, SEARCH_REDUCE, REDUCE, NOOP};
enum Mtype {WORKER, MASTER, CLIENT, NAMESERVER};
enum MState{idle, inproc, cpl};

struct WorkState 
{
   TaskOp   op; 	
   MState   state;  
   int      worker_num;   

   int           num;
   int           offset;
   deque<string> file;
   deque<string> keywords;                   
};

class Machine {
   
protected:
   int         id;
   string      ip;   
   unsigned    port;   
   Mtype       type;   

   string      host_name;   
   WorkState   mstate;
   bool        reg;      
   
public:                    
   Machine();
   Machine(const Machine &_m);
   ~Machine();

   void SetId(int _id);   
   void SetIpAddr(string _ip);
   void SetPortNum(unsigned _port);
   void SetTaskOp(TaskOp _op); 
   void SetNum(unsigned _n);
   void SetWorkerNum(int _n);    
   void SetOffset(unsigned _n);    
   void SetData(Request _q); 
   void SetMachineState(MState _s);
   void SetMachineType(Mtype _t);   
   void SetHostName(string _s);
   void SetKeyword(deque<string> _q); 
   void SetReg(bool _f);          
   void GetLocalhostInfo();
    
   unsigned       GetPortNum();    
   unsigned       GetNum();    
   unsigned       GetOffset(); 
   const char    *GetHostName();   
   deque<string> *GetKeys();  
   deque<string> *GetFileName(); 
   bool           GetReg();              
   Machine        GetNameServer();            
   string         GetIpAddr();  
   TaskOp         GetTaskOp();  
    
   int            GetId(); 
   int            GetWorkerNum();     
                   
   Mtype          GetMachineType(); 
   MState         GetMachineState();   

   const Machine & operator=(const Machine &rhs); 
   ostream & Print(ostream &os) const;
};
inline ostream & operator<<(ostream &os, const Machine & _m) { return _m.Print(os);}                     
 
#endif
