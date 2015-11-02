/**************************
   TinyGoogle project
   task.h
   author: jig26@pitt.edu
   version: 1.0 /11/26/2013 
**************************/
#ifndef _task
#define _task

#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include "connection.h"
#include "request.h"
#include "machine.h"
#include "alg.h"

#define  MAX_THR_NUM	2
class Connection;
class Request;
class Algo;

enum Rpl{ERR=-1, CPL_OK=0, NOP=1};
enum StatOp{Rcvd, Dispd, Reqd, Rpld};

struct lock 
{
   int    id; 	
   string flag;  
};

typedef struct ThreadInfo{   
    Algo *threadclass;   
    void *data;   
}threadinfo_t;   

class MapCall{   
public:   
    static void *Fun(void *data){   
        threadinfo_t *info=(threadinfo_t *)data;   
        return info->threadclass->SearchWord(info->data);   
    }   
};  

class ReduceCall{   
public:   
    static void *Fun(void *data){   
        threadinfo_t *info=(threadinfo_t *)data;   
        return info->threadclass->ReduceIndex(info->data);   
    }   
};

class SearchCall{   
public:   
    static void *Fun(void *data){   
        threadinfo_t *info=(threadinfo_t *)data;   
        return info->threadclass->SearchIndex(info->data);   
    }   
};

class Task {
protected:
   int       id;
   TaskOp    op; 
   Machine   ns;   
   Machine   master;  
   fd_set  * rfds;      
   int     * fmax;  
   FILE    * fstat;    //for performance statistics
   deque <Machine>    workers;
   deque <Connection> connections;
   deque <Request>    rq;
   deque <lock>       lockq;
   deque <lock>       waitq;   
        
public:
   int MAX_WORK_NUM;   
   int WORK_PER_MASTER;	
	      
   Task(Machine _m, TaskOp _op, fd_set * _rfds, int * _fmax);  
   Task();
   ~Task();     
        
   Rpl  HandleIncomingReq(int _fd[][2],fd_set * _rs);           //process incoming request              
   Rpl  HandleReq(int _fd[][2],Request _q);                     //process incoming request
                                                                
 	 int  HandleTaskCpl(Request _q);                              //after task is complete, handle current task state  
   void HandlePipeOutput(int _fd[][2],ReqType _t,int _p);       //send to parent process pipe info
   void HandlePipeInput(int _fd[][2], fd_set * _rs);            //read child process pipe info          
                                                                   
   void AddConn(Connection _c);                                 //add connection
   void AddWorker(Machine _w);                                  //add a worker to worker queue
	 void RemoveCon(Connection _c);                               //remove a connection
   void SortKeys();
   	                                                                 
   void SetTaskOp(TaskOp _op);                                  //Set task operation
   void SetTaskId(int _id);   	                                //allocate new Task id 
   void SetWorkerInfo();                                        
   void SetSearchKeys(Request _q);  
   void SetFd(fd_set * _f);
   void SetFdmax(int * _m);                                           
                                                                      
    int GetTaskId();                                            //get task id    
    int GetWorkers();                                           //get worker from name server       
    int GetMaxSock();                                           //get the max. socket number

   Task * SetNewTask(Request _q);                               //generate a new task
   Task * ProcessNewReq();	  
             	                                                
   deque<Machine>::iterator    GetFreeWorker();                 //find available worker
   deque<Connection>::iterator GetMatchConn(Connection _c);  
   deque<Connection>::iterator GetClientConn();  
   		   
   int  FindMatchConn(Connection _c,fd_set * _rs);               //find match connection                                                          
	 int  DoTask(ReqType _t);                                      //search and list all the words in the document
                                                                 
	 int  UpdateTaskState(Request _q);                             //update machine state
	 int  GenRequest(ReqType _type, Connection _c);                //generate reply to request 
   int  CheckValReq(Request _q);                                 //check validity of a request
   int  ChkAllWorkerState(MState _s);                            //check all machine state    
   int  Register();                                              //register in name server
   int  ReRegister();
   int  InitWorkerReq(ReqType _r);
   int  SetAllWorkerState(MState _s);
   int  GetIndexFile();
	 Rpl  DivTask();                                               //split job to workers  
   Rpl  InitRequest(int _fd[][2]);                               //for master, generate search & index request to workers
   int  RetWorker(int _id);                                      //return worker to master sever
   int  ChkWorkDone(Request _q);    
   int  ChkWorkerNum();
   bool ChkRegStat();   
	 void LockIndex(int _n, const char * _s, int _fd[][2],int _f);

   //do the statics
   void OpenStatFile(int _f);
   void WriteReq (Request _q);
   void WriteMch ();
   void WriteStat(char * _s);
   void CloseStatFile();
   	  		   	      
   ostream & Print(ostream &os) const;   
};

inline ostream & operator<<(ostream &os, const Task & _m) { return _m.Print(os);} 
#endif
