/**************************
   TinyGoogle project
   task.cc
   author: jig26@pitt.edu
   version: 1.0 /11/26/2013 
**************************/
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <sstream> 
#include <fstream>
#include <string>
#include <dirent.h>
#include <string.h>
#include <pthread.h>  
#include "task.h"
#include "global.h"
using namespace std;

Task::Task(){workers.clear();}

Task::Task(Machine _m, TaskOp _op, fd_set * _rfds, int * _fmax) : 
	         master( _m), op(_op), rfds(_rfds), fmax(_fmax){workers.clear();}

Task::~Task(){}
	
void Task::AddConn(Connection _c)
{
    connections.push_back(_c);
}

void Task::SetTaskId(int _id)
{
    id = _id;
}

void Task::AddWorker(Machine _w)
{
	  int size = (int)workers.size();
	  _w.SetId(size);
    workers.push_back(_w);
}

int Task::FindMatchConn(Connection _c, fd_set * _rs)
{  
   for(int i = 0; i <= *fmax; i++) 
   {    
       if(FD_ISSET(i, _rs)) 
       {
       	if(_c.GetSocket() == i) 
          return(1); 
       }
   }
   return(0);      
}

deque<Connection>::iterator Task::GetMatchConn(Connection _c)
{  
   deque<Connection>::iterator it;	
   for(it = connections.begin(); it != connections.end();it++)
   {
   	 if((*it).GetDest()!= NULL && (_c).GetDest()!= NULL)
   	 	{
   	   string s1 = (*it).GetDest()->GetHostName();
   	   string s2 = _c.GetDest()->GetHostName();
   	   if(s1.compare(s2)==0) return(it);
      }   
   } 
   return(it);      
}

deque<Connection>::iterator Task::GetClientConn()
{  
   deque<Connection>::iterator it;	
   for(it = connections.begin(); it != connections.end();it++)
   {
   	 if(it->GetSockType() == CLIENT_CONNECTION)
   	 {
   	   return(it);
   	 }
   } 
   return(it);      
}

Rpl Task::HandleReq(int _fd[][2],Request _q)
{	  
	  Rpl cpl = NOP; deque<string>::iterator it;deque<string> k;
	  
	  if(Task::CheckValReq(_q) == -1) 
	  { 
	  	 cerr << "A wrong request is recognized!\n" << endl;
	  	 return (ERR);  		  	
	  }	  		  	  
	  switch(_q.GetReqType())
	  {
	  	case INDEX_REQ :
	  	case SEARCH_REQ:    	  		   
	  		   rq.push_back(_q); Task::WriteReq (_q); Task::WriteStat("Rcvd");
	  		   break;	  		  
	  	case INDEX_MAP_REQ:
	  		   Task::WriteReq (_q);Task::WriteStat("Rcvd");  
	  		   Task::UpdateTaskState(_q); 
  		     Task::DoTask(INDEX_MAP_REQ);
  		     Task::GenRequest(INDEX_MAP_REPLY,_q.GetConn()); 
  		     Task::WriteStat("Cpl"); break;
	  	case INDEX_REDUCE_REQ:
	  		   Task::WriteReq (_q); Task::WriteStat("Rcvd"); 
  		     Task::UpdateTaskState(_q); 
  		     Task::DoTask(INDEX_REDUCE_REQ);
  		     Task::GenRequest(INDEX_REDUCE_REPLY,_q.GetConn());
      	   Task::HandleTaskCpl(_q); 
      	   cpl = CPL_OK; Task::WriteStat("Cpl"); break;	
	  	case SEARCH_REDUCE_REQ:
  		     Task::WriteReq (_q);Task::WriteStat("Rcvd");
  		     Task::UpdateTaskState(_q); 
  		     Task::DoTask(SEARCH_REDUCE_REQ);  		    	
  		     Task::GenRequest(SEARCH_REDUCE_REPLY,_q.GetConn());
      	   Task::HandleTaskCpl(_q); cpl = CPL_OK;  
      	   Task::WriteStat("Cpl");	break;	  		 	  	     
      case INDEX_MAP_REPLY:	
	  		   if(Task::UpdateTaskState(_q)) 
	  		   	 {Task::HandlePipeOutput(_fd,PERMIT_REQ,id);
	  		   	 	Task::WriteStat("Index_map_cpl");} break;
      case PERMIT_REPLY:	  		   
	         switch(op){
          	     case REDUCE: Task::DivTask();Task::InitWorkerReq(INDEX_REDUCE_REQ); break;
	               case SEARCH: Task::InitWorkerReq(SEARCH_REDUCE_REQ);break;	     	
	   	           default:;}	
	   	     Task::WriteStat("Perm_cpl"); break;
	    case RLS_REQ:
	         Task::RetWorker(_q.GetReqNum()); k = _q.GetReqKey(); it = k.begin(); 
	  		   Task::LockIndex(_q.GetReqNum(),(*it).c_str(),_fd,0);       
	  		   break;	
      case PERMIT_REQ:    	
      	   k = _q.GetReqKey(); it = k.begin(); 
	  		   Task::LockIndex(_q.GetReqNum(),(*it).c_str(),_fd,1);
	  		   Task::WriteStat("Perm_req"); break;	//1:lock
      case PERMIT_RLS_REQ:  
      	   k = _q.GetReqKey(); it = k.begin(); 
	  		   Task::LockIndex(_q.GetReqNum(),(*it).c_str(),_fd,0);       //0:lock
	  		   break;		  		   	            	       	  	     		
      case INDEX_REDUCE_REPLY: 
	  		   if(Task::UpdateTaskState(_q)){
             deque<string> * q = master.GetFileName(); (*q).pop_front();
             Task::WriteStat("Index_reduce_cpl");  
	  		     if(!(*q).size()){//no remaining files
	  		     	    Task::WriteStat("Cpl");
	  		          deque<Connection>::iterator c = Task::GetClientConn();
	  		          if(c == connections.end()){
        	           cerr << "Fatal Error: client is not found in HandleReq()!!" << endl;
        	           exit(-1);}	  		      
	  		   	      Task::GenRequest(INDEX_REPLY,*c);
	  		   	      Task::HandleTaskCpl(_q);cpl = CPL_OK;}
	  		   	 else{
	  		     	    Task::HandlePipeOutput(_fd,PERMIT_RLS_REQ,id);	  		   	 	
	  		   	   	  op=INDEX; master.SetTaskOp(INDEX);
	  		   	   	  Task::DivTask();InitWorkerReq(INDEX_MAP_REQ);} 
	  		   } break;		  		   	       	
      case SEARCH_REDUCE_REPLY://furture: sort all results
	  		   if(Task::UpdateTaskState(_q)){
	  		   	  Task::SortKeys(); Task::WriteStat("Cpl"); 
	  		      deque<Connection>::iterator c = Task::GetClientConn();
	  		      if(c == connections.end()){
        	      cerr << "Fatal Error: client is not found in HandleReq()!!" << endl;
        	      exit(-1);}	 
	  		   	  Task::GenRequest(SEARCH_REPLY,*c);
      	      Task::HandleTaskCpl(_q); cpl = CPL_OK; 
      	   }  break;	
      case WORKER_REPLY:
      case REG_REPLY:      	
	         Task::UpdateTaskState(_q); break;
	  	default:;
	  } 
	  return(cpl);
}
   
Rpl Task::HandleIncomingReq(int _fd[][2],fd_set *_rs)
{
    Rpl cpl = NOP;
    for(deque<Connection>::iterator it = connections.begin();it!= connections.end();it++)
    {
     if(Task::FindMatchConn(*it, _rs))
     {
        switch(it->GetSockType())
        {
          case ACTIVE_CONNECTION:
          case CLIENT_CONNECTION:
          	   {          
                Request q(*it); 
                if(q.GetRequest()!=-1)
                 { cpl = Task::HandleReq(_fd,q); }
                else
                 {
                 	(*it).ConClose(rfds, fmax); Task::RemoveCon(*it);cpl = CPL_OK;
                 }
               }  break;
          case LISTEN_CONNECTION:
               {
                  Connection c(&master,&master,CLIENT_CONNECTION,TCP);
                  c.ConAccept(rfds,fmax,it->GetSocket());
                  this->AddConn(c);  
               }  break;         
          default:;
        }
        if(cpl == CPL_OK) break;
     }
    }
    return(cpl);
}

deque<Machine>::iterator Task::GetFreeWorker()
{
	  deque<Machine>::iterator it;
    if(workers.size() != MAX_WORK_NUM*WORK_PER_MASTER )
      {
      	cerr << "Warning: No worker (Master) is correct in GetFreeWorker() !!" << endl;
      	exit(-1);
      }
    
    for(it = workers.begin(); it != workers.end();it++)
    {
      if((*it).GetMachineState() == idle) return (it); 	
    }
    return (it);      
}

int Task::UpdateTaskState(Request _q)
{
    int c_ok = 0; deque<string> q; q.clear();//1:all task is finished
	  master.SetData(_q);	  
	  switch(_q.GetReqType()){
	  	case INDEX_REQ :    Task::SetTaskOp(INDEX);	   break;	
	  	case SEARCH_REQ:    Task::SetTaskOp(SEARCH);   break;	  		   	
	  	case INDEX_MAP_REQ: Task::SetTaskOp(INDEX_MAP);break;	  		
  	  case INDEX_REDUCE_REQ:    Task::SetTaskOp(INDEX_REDUCE);  break;	 
      case INDEX_MAP_REPLY:     if((c_ok=ChkWorkDone(_q))==1){ 
      	   	 	                      master.SetTaskOp(REDUCE);
      	   	 	                      Task::SetTaskOp(REDUCE);
      	   	 	                      master.SetNum(26);}         break;	 	  		   	     		      	  	  	  	 	  		  
      case SEARCH_REDUCE_REQ:   Task::SetTaskOp(SEARCH_REDUCE); break;
      case INDEX_REDUCE_REPLY:  c_ok=Task::ChkWorkDone(_q); break;
      case SEARCH_REDUCE_REPLY: c_ok=Task::ChkWorkDone(_q); 
      	                        Task::SetSearchKeys(_q);	  break;
      case WORKER_REPLY:        if(workers.size() != MAX_WORK_NUM*WORK_PER_MASTER &&  
      	                           master.GetNum()!=0) Task::SetWorkerInfo(); break;	
      case REG_REPLY:           master.SetReg(true); break;             	        	  	
	  	default:;
	  } 
	  return(c_ok);
}

void Task::SetSearchKeys(Request _q)
{
	deque<string> q = _q.GetReqKey(); 
	for(deque<string>::iterator it = q.begin(); it!= q.end();it+=2)
	{
	  deque<string> * q1 = master.GetKeys(); int found = 0;
	  deque<string>::iterator itn;
	  for(itn = (*q1).begin(); itn!=(*q1).end();itn+=2)
		{ if(*it == *itn) {found = 1;break;}}
			
	  if(found == 1)
		{
		   int n1 = atoi((*(it+1)).c_str());int n2 = atoi((*(itn+1)).c_str()); 
		   n1 += n2; string s = slib::_itoa(n1); (*(itn+1))=s;continue;
		}
    int n1,n2;
	  (*q1).push_back(*it); (*q1).push_back(*(it+1));
  }
}

void Task::SortKeys()
{
  deque<string> * q1 = master.GetKeys(); deque<string> q2; deque<string> q3; 
  q2.clear(); q3.clear();  
  for(deque<string>::iterator it = (*q1).begin(); it!= (*q1).end();it+=2)	
  {
  	q2.push_back(*it); q3.push_back(*(it+1));  
  } (*q1).clear();
  while(q2.size())
  {
  	deque<string>::iterator itn1,itn2; int n1=0,n2; 
  	deque<string>::iterator it2 = q3.begin();
  	for(deque<string>::iterator it1 = q2.begin(); it1!= q2.end();it1++)	
  	{
  		n2 = atoi((*it2).c_str()); if(n2>n1){itn1 = it1; itn2 = it2; n1 = n2;} 
  	  it2++;
  	}
  	(*q1).push_back(*itn1); (*q1).push_back(*itn2);
  	q2.erase(itn1); q3.erase(itn2);
  } 
}

int Task::ChkWorkDone(Request _q)
{
	  Connection c = _q.GetConn();
    Machine * m = c.GetDest();
    m->SetMachineState(cpl);	
    int c_ok = Task::ChkAllWorkerState(cpl);
    return(c_ok);
}
	
int Task::CheckValReq(Request _q)
{
	  int flag = 0;
	  switch(op)
	  {
	  	case LISTEN :
	  		   if(master.GetMachineType() == MASTER)
	  		   {
	  	  	   if(_q.GetReqType() != INDEX_REQ    && _q.GetReqType() != SEARCH_REQ && 
	  	  	   	  _q.GetReqType() != WORKER_REPLY && _q.GetReqType() != REG_REPLY &&
	  	  	   	  _q.GetReqType() != PERMIT_REQ   && _q.GetReqType() != RLS_REQ   &&
	  	  	   	  _q.GetReqType() != PERMIT_RLS_REQ)
	  	  	   {
	  	  	 	    flag = -1;
	  	  	   }
	  	  	 }  
	  		   if(master.GetMachineType() == WORKER)
	  		   {
	  	  	   if(_q.GetReqType() != INDEX_MAP_REQ && _q.GetReqType() != SEARCH_REDUCE_REQ &&
	  	  	   	  _q.GetReqType() != REG_REPLY)
	  	  	   {
	  	  	 	    flag = -1;
	  	  	   }
	  	  	 }
	  	  	 break;		  	
	  	case INDEX :
	  		   if(master.GetMachineType() == MASTER)
	  		   {
	  	  	   if(_q.GetReqType() != INDEX_MAP_REPLY)
	  	  	   {
	  	  	 	    flag = -1;
	  	  	   }
	  	  	 }  
	  		   if(master.GetMachineType() == WORKER)
	  		   {
	  	  	   cerr << "Fatal Error: Wrong WORKER in INDEX!" << endl;
	  	  	 	 exit(-1);
	  	  	 }
	  	  	 break;	
	  	case SEARCH:
	  		   if(master.GetMachineType() == MASTER)
	  		   {
	  	  	   if(_q.GetReqType() != SEARCH_REDUCE_REPLY &&_q.GetReqType() != PERMIT_REPLY)
	  	  	   {
	  	  	 	    flag = -1;
	  	  	   }
	  	  	 }  
	  		   if(master.GetMachineType() == WORKER)
	  		   {
	  	  	   cerr << "Fatal Error: Wrong worker in SEARCH!" << endl;
	  	  	 	 exit(-1);
	  	  	 }
	  	  	 break;	 
	  	case REDUCE:
	  		   if(master.GetMachineType() == MASTER)
	  		   {
	  	  	   if(_q.GetReqType() != INDEX_REDUCE_REPLY &&_q.GetReqType() != PERMIT_REPLY)
	  	  	   {
	  	  	 	    flag = -1;
	  	  	   }
	  	  	 }  
	  		   if(master.GetMachineType() == WORKER)
	  		   {
	  	  	   cerr << "Fatal Error: Wrong worker for REDUCE!" << endl;
	  	  	 	 exit(-1);
	  	  	 }
	  	  	 break;		  	  	  		   	
	  	case INDEX_MAP:
	  		   if(master.GetMachineType() == WORKER)
	  		   {
	  	  	   if(_q.GetReqType() != INDEX_REDUCE_REQ)
	  	  	   {
	  	  	 	    flag = -1;
	  	  	   }
	  	  	 }  
	  		   if(master.GetMachineType() == MASTER)
	  		   {
	  	  	   cerr << "Fatal Error: Wrong master in INDEX_MAP!" << endl;
	  	  	 	 exit(-1);
	  	  	 }
	  	  	 break;  		
      case INDEX_REDUCE:
	  		   cerr << "Fatal Error: Wrong worker/master in INDEX_REDUCE!" << endl;
	  	  	 exit(-1);  		
	  	  	 break;	  	  	 	  			  		  	      	  	  	  	 	  		    	        	  	
	  	default:;
	  } 
	  return(flag);
}

int Task::HandleTaskCpl(Request _q)
{	
	  if(master.GetMachineType() == MASTER)
	  {
	    if(op != SEARCH && op != REDUCE)
	    {
	    	cerr << "Fatal Error: Master Op is NOT correct! in TASK::HandleTaskCpl()" << endl;
	    	exit(-1);
	    }
	  }
	  if(master.GetMachineType() == WORKER)
	  {
	    if(op != SEARCH_REDUCE && op != INDEX_REDUCE)
	    {
	    	cerr << "Fatal Error: Worker Op is NOT correct! in TASK::HandleTaskCpl()" << endl;
	    	exit(-1);
	    }
	  }    
	  //master have to wait to other connection close signal
	  if(master.GetMachineType() == MASTER)
	  {	      
	    while(connections.size())
	    {    
	      deque<Connection>::iterator it = connections.begin();
	      Task::RemoveCon(*it);(*it).ConClose(rfds, fmax);
	    }	 	    
	  }

	  if(master.GetMachineType() == WORKER)
	  {
      Task::SetTaskOp(LISTEN); op = LISTEN;                    //set task state            	               		    
	  } 	  	  
}

void Task::SetTaskOp(TaskOp _op)
{
    op = _op;
}

int Task::GetTaskId()
{
	return(id);
}

int Task::GetMaxSock()
{
    int fm = 0;
	  for(deque<Connection>::iterator it = connections.begin(); it!= connections.end();it++)
	  { 
        if((*it).GetSocket()>fm) 
        	fm = (*it).GetSocket();
    }  
    return(fm);
}

void Task::RemoveCon(Connection c)
{
    FD_CLR(c.GetSocket(), rfds);               //remove the connection from fs_set
    int fm = c.GetSocket(); 
    deque<Connection>::iterator it;
    it = GetMatchConn(c);	
    connections.erase(it);                     
    if(fm == *fmax)
      *fmax = Task::GetMaxSock();
}

Rpl Task::InitRequest(int _fd[][2])
{	  
    deque<Connection>::iterator itn = connections.begin();
    int s = (*itn).GetSocket();
    FD_ZERO(rfds);FD_SET(s,rfds);*fmax=s;               	//Reset fd lists for child process
    master.SetWorkerNum(WORK_PER_MASTER);                 //Reset worker number
    (*itn).SetSrc(&master);(*itn).SetDest(&master);       //folk: copy-on-write update the pointer
    Task::SetAllWorkerState(cpl);
    deque<string> * h = master.GetKeys();                 //record host name for index_reduce 
    if(op!= SEARCH)(*h).clear();   
	  if(op == INDEX)
	  {
	    if(Task::GetIndexFile() == -1)
	    {   	      
         GenRequest(INVALID_REQ_REPLY, *itn);
         return(ERR);
      }
    }
	  if(Task::DivTask()==ERR)return(ERR);	
    	  	
	  deque<Machine>::iterator it = workers.begin();
	  for(;it!= workers.end();it++)
	  { 
	  	 if((*it).GetMachineState()==inproc){
	  	   Connection c(&master,&(*it),ACTIVE_CONNECTION,TCP);
	  	   c.SetDestPort((*it).GetPortNum());
	  	   c.ConSetup(rfds,fmax,1);  c.ConConnect();
	  	   connections.push_back(c); 
	  	   if(op!= SEARCH)(*h).push_back((*it).GetHostName());//to record host name for index reduce
	     }
	  }
	  
	  switch(op)
	  {
	  	 case INDEX:  InitWorkerReq(INDEX_MAP_REQ);        break;   
	     case REDUCE: InitWorkerReq(INDEX_REDUCE_REQ);     break;
	     case SEARCH: HandlePipeOutput(_fd,PERMIT_REQ,id); break;	     	
	   	 default:;
	  }	  
    return(NOP);
}

int Task::GenRequest(ReqType _type, Connection _c)
{
	  Request q(_c);
    q.SetReqType(_type);
    q.SetMessage(_type, _c, 0);
    q.SendMessage();
    return(0);    
}

Rpl Task::DivTask()
{  
	Rpl s = ERR;    
  if(master.GetTaskOp() == INDEX)
  {
  	deque<string> *md = master.GetFileName();
    deque<string>::iterator f = (*md).begin();                  //Get file size
    string fn = *f;
    int size; int offset = 0; int i = 0;     	
    struct stat st; stat(fn.c_str(), &st);
    size = st.st_size; 
    if(size == 0)
    	{cerr << "Warning:Task file size is "<< size << endl; return(ERR);}
    for(deque<Machine>::iterator it = workers.begin();it!= workers.end();it++)
    {
    	 deque<string> *d = (*it).GetFileName();
    	 (*it).SetMachineState(inproc); 
    	 (*d).clear();(*d).push_back(fn);                          //set the file directory 
  	   int n  = slib::getlen(size,WORK_PER_MASTER,i);
       int n1 = slib::getoffset(offset,size,WORK_PER_MASTER,i);  	   	
  	   (*it).SetNum(n);	(*it).SetOffset(n1); 
  	   (*it).SetTaskOp(INDEX_MAP);                               //set offset       
  	   i++;  s = NOP;
    }  
  }
  
  if(master.GetTaskOp() == SEARCH)
  {
  	deque<string> *md = master.GetKeys();
    deque<Machine>::iterator w = workers.begin(); 
    	
    for(deque<string>::iterator it = (*md).begin();it!= (*md).end();it++)
    {    
    	 int num = w->GetNum(); num++; w->SetNum(num);
       w->SetTaskOp(SEARCH_REDUCE); 
    	 w->SetMachineState(inproc);           	 
    	 deque<string> *q = w->GetKeys();   
       (*q).push_back(*it);
       if(w+1 == workers.end()) w = workers.begin();
       else w++; s = NOP;
    } 
    (*md).clear();//clear it to store the search result
  } 
 
  if(master.GetTaskOp() == REDUCE)
  {
    int size=master.GetNum(); int offset = 0; int i = 0;
    for(deque<Machine>::iterator it = workers.begin();it!= workers.end();it++)
    {
    	 (*it).SetMachineState(inproc);                            //set the file directory 
  	   int n  = slib::getlen(size,WORK_PER_MASTER,i);
       int n1 = slib::getoffset(offset,size,WORK_PER_MASTER,i);
       (*it).SetNum(n);	(*it).SetOffset(n1);                     //set offset
       (*it).SetTaskOp(INDEX_REDUCE);
  	   i++;  s = NOP;
    }  
  } 
      
  return(s);
}

void Task::HandlePipeOutput(int _fd[][2], ReqType _t, int _p)
{
  int pipe;                                             //release task id <release>,<id>
	if(op == LISTEN) pipe=_fd[MAX_WORK_NUM+_p][1];        //to child process
	else pipe=_fd[_p][1];	
			
	Connection c(&master,_p); Request q(c); 
  q.SetReqType(_t); q.SetMessage(_t, c,id); cerr << "pipe request is " << q << endl;
  deque<string> s=q.GetReqFile(); string m = *(s.begin()); 

  if(m.size() > 200)
  {
    cerr << "Fatal Error: copied data size exceeds buffer size in HandlePipeOutput()!"<< endl;
    exit(-1);
  }
  if(write(pipe,m.c_str(),m.size())<0)
  {
    cerr << "Fatal Error: message sending failed in HandlePipeOutput()!"<< endl;
    exit(-1);
  }
}

void Task::HandlePipeInput(int _fd[][2],fd_set *_rs)
{   
  for(int i = 0; i <= *fmax; i++) 
  {
      if (FD_ISSET(i, _rs)) 
      	 for(int j = 0; j<MAX_WORK_NUM*2; j++)
      	 {
            if(_fd[j][0] == i)//Ready to receive messege
            {  
            	Connection c(&master,i); 
            	Request q(c); if(q.GetRequest()!=-1) HandleReq(_fd,q); break;
            }           	
         }
  }
}

int Task::RetWorker(int _id)
{	
	Task *t = NULL;
  unsigned id_start = (_id+0)*WORK_PER_MASTER;
  unsigned id_end   = (_id+1)*WORK_PER_MASTER-1;  

  for(deque<Machine>::iterator it = workers.begin();it!= workers.end();it++)
  {   
     if((*it).GetId() >= id_start && (*it).GetId() <= id_end)
     {
        if((*it).GetMachineState()!= inproc)
        {
          cerr << "Fatal Error: machine state is NOT correct in RetWorker()!"<< endl;
          exit(-1);  
        }
        (*it).SetMachineState(idle);
     }
  }
  
  return (0);
}

int Task::DoTask(ReqType _t)
{
   pthread_t thread[MAX_THR_NUM]; pthread_attr_t attr;
   int rc; long t; void *status;  Algo subtask(&master);
   pthread_mutex_t lock; threadinfo_t info[MAX_THR_NUM]; arg_t a[MAX_THR_NUM];
   /* Initialize and set thread detached attribute */
   pthread_attr_init(&attr);
   pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
   Task::WriteStat("Thread_start"); 
   if (pthread_mutex_init(&lock, NULL) != 0){printf("\n mutex init failed\n");exit(-1);}
      for(t=0; t<MAX_THR_NUM; t++) {
         a[t].id=t; a[t].total=MAX_THR_NUM; a[t].lock=&lock;
         info[t].threadclass=&subtask;   
         info[t].data=(void *)&(a[t]); 
         switch(_t)
         {      
           case INDEX_MAP_REQ:
                rc = pthread_create(&thread[t], &attr, &MapCall::Fun, (void *)&(info[t]));
                printf("IndexMap: creating thread %ld\n", t);break; 
           case INDEX_REDUCE_REQ:
                rc = pthread_create(&thread[t], &attr, &ReduceCall::Fun, (void *)&(info[t]));
                printf("IndexReduce: creating thread %ld\n", t);break; 
           case SEARCH_REDUCE_REQ:
                rc = pthread_create(&thread[t], &attr, &SearchCall::Fun, (void *)&(info[t]));
           	    printf("Search: creating thread %ld\n", t);break;  
           default:;
         }      	      	
         if (rc) { printf("ERROR: return code from pthread_create() is %d\n", rc);exit(-1);}
      }
   
      /* Free attribute and wait for the other threads */
      pthread_attr_destroy(&attr);
      for(t=0; t<MAX_THR_NUM; t++) {
         rc = pthread_join(thread[t], &status);
         if (rc) {printf("ERROR: return code from pthread_join() is %d\n", rc); exit(-1);}
      }  Task::WriteStat("Thread_cpl"); 
      
   switch(_t)
   {      
     case INDEX_MAP_REQ:    
     	    subtask.FlushMap();  break; 
     case INDEX_REDUCE_REQ:    break; 
     case SEARCH_REDUCE_REQ:
     	    subtask.UpdateSearchIndex(); deque<string> *q = subtask.GetFilebyCount();
     	    master.SetKeyword(*q); master.SetNum((*q).size()>>1);delete q;break;
   } 
   return(0);
}

int Task::GetWorkers()
{
	  master.SetNum(MAX_WORK_NUM*WORK_PER_MASTER);    
	  Connection c(&master, &ns, ACTIVE_CONNECTION,UDP);
    deque<Connection>::iterator it = GetMatchConn(c);
	  if(it != connections.end()) Task::GenRequest(WORKER_REQ,*it);
	  else {cerr << "Fatal error!!" << endl; exit(-1);}
	  return(0);
}

int Task::ReRegister()
{
	  Connection c(&master, &ns, ACTIVE_CONNECTION,UDP);
    deque<Connection>::iterator it = GetMatchConn(c);
	  if(it != connections.end()) Task::GenRequest(REG_REQ,*it);
	  Task::GenRequest(REG_REQ,*it);	
	  return(0);	                  
}

Task * Task::SetNewTask(Request _q)
{
	  Task *t = NULL;	       
    master.SetData(_q);
            
    if(_q.GetReqType() == INDEX_REQ )
    	t = new Task(master,INDEX,rfds,fmax);  
    else if (_q.GetReqType() == SEARCH_REQ)
	    t = new Task(master,SEARCH,rfds,fmax);
    t->AddConn(_q.GetConn());     
    if(t){t->MAX_WORK_NUM=MAX_WORK_NUM;t->WORK_PER_MASTER=WORK_PER_MASTER;}
    return(t);   
}

int Task::Register()
{
	ns = master.GetNameServer(); 
	Connection c(&master, &ns, ACTIVE_CONNECTION,UDP);
  c.SetDestPort(ns.GetPortNum());	c.ConSetup(rfds,fmax,1);
	this->AddConn(c);	       
	Task::GenRequest(REG_REQ,c);	
	return(c.GetSocket());	                  
}

int Task::ChkAllWorkerState(MState _s)
{
	int cpl_num=0;
  for(deque<Machine>::iterator it = workers.begin();it!= workers.end();it++)
  {
  	 if(it->GetMachineState()== _s)
  	 	 cpl_num++; 
  } 	  
  if(cpl_num == workers.size()) return(1);
  else return(0);
}

int Task::SetAllWorkerState(MState _s)
{
  for(deque<Machine>::iterator it = workers.begin();it!= workers.end();it++)
     it->SetMachineState(_s); 	  
}

int Task::InitWorkerReq(ReqType _r)
{	
	  for(deque<Connection>::iterator it = connections.begin(); it!= connections.end();it++)
	  { 
	  	 if(it->GetSockType()== ACTIVE_CONNECTION)
       {	
	  	    GenRequest(_r,*it);   
	     }
	   else if (it->GetSockType()== CLIENT_CONNECTION){}
    }
}

int Task::GetIndexFile()
{
	  deque<string>* f = master.GetFileName();
	  deque<string>::iterator s = (*f).begin();
    deque<string> q; q.clear(); DIR  *d; 
    struct dirent *dir; 
    d = opendir((*s).c_str());
    if (d)
    {
      while ((dir = readdir(d)) != NULL)
      {
       if(strcmp(dir->d_name,".")==1 && strcmp(dir->d_name,"..")==1)
       	{
         string s1; s1.insert(0,*s); s1.push_back('/');
         s1.insert(s1.size(),dir->d_name) ;
         q.push_back(s1); printf("%s\n", dir->d_name);
        }
      }
     (*f) = q; closedir(d); 
     return(0);
    }	
    return(-1);   	 
}

Task* Task::ProcessNewReq()
{
	  Task *t=NULL;
    deque<Machine>::iterator it = Task::GetFreeWorker();
    deque<Request>::iterator q = rq.begin(); 
  	if(it!=workers.end() && q!=rq.end())
  	{
  		 t = Task::SetNewTask(*q); 
	     if(!t)
	     {
	     	  cerr << "Fatal Error:GetNewTask fails!!" << endl;
          exit(-1);
	     }	
	     for(int i=0;i<WORK_PER_MASTER;i++)
	     {
            if(it->GetMachineState()!= idle || it == workers.end())
            {
             cerr << "Fatal Error: worker state is NOT correct in HandleReq()!"<< endl;
             exit(-1);      	
            }
            it->SetMachineState(inproc);
            it->SetNum(0); int wid = it->GetId();
            t->AddWorker(*it); t->SetTaskId(wid/WORK_PER_MASTER); it++;                
	     } 	       		     	
	     rq.pop_front();	
	  }
	 return(t);
}

void Task::SetWorkerInfo()
{	
	 deque<string> * q = master.GetKeys();
	 deque<string>::iterator it; Machine wm;int i=0;
	 master.SetWorkerNum(MAX_WORK_NUM*WORK_PER_MASTER);
   for(it=(*q).begin(); it!=(*q).end();it++)//0:ip 1:port 2:host name
   {
      if(i%3==1)      
      	 wm.SetPortNum(atoi((*it).c_str()));                
   	  else if(i%3==2)
   	  {  wm.SetHostName(*it); Task::AddWorker(wm); 
   	  	 cerr << "New worker is added:\n" << wm << endl;}  
   	  i++;
   }   
   if(i!=3*master.GetWorkerNum() || i%3!=0)
   {
	    cerr << "Fatal Error:  is NOT consistent with i in SetWorkerInfo()!!" << endl;
      exit(-1);      	   
   }  
}

bool Task::ChkRegStat()
{
	return(master.GetReg());
}	

int  Task::ChkWorkerNum()
{
	return(master.GetWorkerNum());
}
         	 
void Task::LockIndex(int _n, const char * _s, int _fd[][2], int _f)
{
 struct lock l; l.id=_n; l.flag=_s; int permit = 0; 
 deque<lock>::iterator it = lockq.begin();

 if(_f==0) { //0: unlock 1: lock
 	  for(;it!=lockq.end();it++){
 	     	if(it->id == l.id) {lockq.erase(it); break;} 
 	  }
 	  it = lockq.begin();  deque<lock>::iterator itn = waitq.begin();
    if((waitq.size()!=0 && lockq.size()==0) ||
    	 (waitq.size()!=0 && lockq.size()!=0  && 
    	  it->flag.compare("read")==0 && (*itn).flag.compare("read")==0)) 
    	{ permit = 1; l.id = (*itn).id; l.flag = (*itn).flag; 
    		lockq.push_back(l); waitq.pop_front();}
 }	             
 else{
    it = lockq.begin();
    if(lockq.size()==0) 
    	{permit = 1; lockq.push_back(l);}
    else if(it->flag.compare("read")==0 && l.flag.compare("read")==0) 
    	{permit = 1; lockq.push_back(l);}
 	  else waitq.push_back(l); 
 }
 
 if(permit)HandlePipeOutput(_fd,PERMIT_REPLY,l.id);
}	
 
void Task::SetFd(fd_set * _f)
{
	rfds = _f;
}

void Task::SetFdmax(int * _m)
{
	fmax = _m;
}  

void Task::OpenStatFile(int _f)
{
  struct timeval tv; struct timezone tz; gettimeofday (&tv,&tz);
  string s1 = slib::_itoa(tv.tv_sec);  
	string s  = "./tmp/stat_";  
	s.insert(s.size(),master.GetHostName());
	switch(_f) 
  {
		 case 0: s.insert(s.size(),"_dispatcher_"); break;
	   case 1: s.insert(s.size(),"_master_");     break;
	}
  s.insert(s.size(),s1);
	fstat = fopen(s.c_str(),"w"); 
	if(!fstat)	{cerr << "Opening fstat file fails!\n" << endl; exit(-1);}
}

void Task::WriteReq (Request _q)
{
  switch(_q.GetReqType())
  {
  	case SEARCH_REQ:        fprintf(fstat, "req: search ");       break;
  	case SEARCH_REDUCE_REQ:	fprintf(fstat, "req: search_reduce ");break;
  	case INDEX_MAP_REQ:     fprintf(fstat, "req: index_map ");    break;
  	case INDEX_REDUCE_REQ:  fprintf(fstat, "req: index_reduce "); break;				  		   
  	case INDEX_REQ :        fprintf(fstat, "req: index  ");       break;
  }
  if(_q.GetReqType() == SEARCH_REQ || _q.GetReqType() == SEARCH_REDUCE_REQ) {
  	 deque<string> f = _q.GetReqKey();  
     for(deque<string>::iterator it = f.begin();it!=f.end();it++)
        fprintf(fstat,"%s ", it->c_str()); fprintf(fstat,"\n");
  }
  else if(_q.GetReqType() == INDEX_REDUCE_REQ) 
  	 fprintf(fstat,"\n"); 
  else{
  	  string f = *((_q.GetReqFile()).begin());  
  		fprintf(fstat,"%s\n", f.c_str());
  } 
}

void Task::WriteMch()
{
  switch(master.GetTaskOp())
  {
  	case INDEX:   fprintf(fstat, "req: index  ");break;
  	case SEARCH:	fprintf(fstat, "req: search ");break;
  }
  if(master.GetTaskOp() == SEARCH) {
  	 deque<string> *f = master.GetKeys();  
     for(deque<string>::iterator it = (*f).begin();it!=(*f).end();it++)
        fprintf(fstat,"%s ", it->c_str()); fprintf(fstat,"\n");
  }
  else{
  	  string f = *((*(master.GetFileName())).begin());  
  		fprintf(fstat,"%s\n", f.c_str());
  } 
}

void Task::CloseStatFile(){fclose(fstat);}
   
void Task::WriteStat (char * _s)
{
  //<op:rcvd> <rcvd ts>
  struct timeval tv; struct timezone tz; gettimeofday (&tv,&tz);
  double t = (double)tv.tv_sec + (double)tv.tv_usec/1000000.0;  
  fprintf(fstat,"op: %s %lf\n",_s,t);
}
          
ostream & Task::Print(ostream &os) const
{
	 deque<Machine>::const_iterator it; int i=1;
   os << "Task(id="<<id<<", op="<<op<<" \n"
      << "      master: " << master <<" \n"
      << "          ns: " <<ns <<endl;
   
   os << "workers(\n";    
   for(it = workers.begin(); it != workers.end();it++)
   { 
      os <<"id "<< i << ":\n" <<(*it); i++;  	  	      
   }   
   os <<" )" << " \n"; 
   os <<" )" << " \n";  
   return os;
}
