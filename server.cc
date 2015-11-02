/**************************
   TinyGoogle project
   server_module.cc
   argments: binary master/worker
   author: jig26@pitt.edu
   version: 1.0 /11/26/2013 
**************************/
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <stdio.h>
#include <string.h>
#include <netdb.h>
#include <sys/time.h>
#include <stdlib.h>
#include <ctype.h>
#include <iostream>
#include <string>
#include "task.h"

using namespace std;

int main(int argc, char * argv[]) {
                                                     
    fd_set  fd_sets,master;    
    int     fmax;     
    int     max_disp_no = atoi(argv[2]);
    int     max_work_no = atoi(argv[3]);     
    int     pipe_fd[max_disp_no*2][2];
        
    struct  timeval timeout;
    timeout.tv_sec =  10;
    timeout.tv_usec = 0;
   
    Task *  new_task;                                                  
    pid_t   child_pid;
    
    //initialize fd table
    FD_ZERO(&fd_sets);fmax = 0;
    
    //parent process only read    
    for(int i=0; i<max_disp_no*2;i++) pipe(pipe_fd[i]);  	   	
    for(int i=0; i<max_disp_no;i++)
    {    	   	
    	FD_SET(pipe_fd[i][0],&fd_sets);             
    	if(fmax < pipe_fd[i][0]) 
    		 fmax = pipe_fd[i][0]; 
    }
             
    //detemine worker/master
    Machine  host;
    if(strcmp(argv[1],"master") == 0)
       host.SetMachineType(MASTER);
    if(strcmp(argv[1],"worker") == 0)
       host.SetMachineType(WORKER);
    host.GetLocalhostInfo(); cerr << host << endl;
   
    //Create a task only for listening
    Connection l(&host,NULL,LISTEN_CONNECTION,TCP);                              
    l.ConSetup(&fd_sets,&fmax,0); l.ConListen(); 

    Task t(host,LISTEN,&fd_sets,&fmax);
    t.OpenStatFile(1);
    t.AddConn(l);cerr << t;  
    t.MAX_WORK_NUM = atoi(argv[2]);
    t.WORK_PER_MASTER = atoi(argv[3]);                                          
    //register in the name server
    int u = t.Register();
    if(strcmp(argv[1],"master") == 0)
      t.GetWorkers();

    //resend the worker request/register request
    while(1){
    	  int cpl = 1; master = fd_sets;
        if(select(fmax + 1, &master, NULL, NULL, &timeout)>0) 
          {t.HandleIncomingReq(pipe_fd,&master);}   	  
        if(strcmp(argv[1],"master") == 0)
        {  
        	if(t.ChkWorkerNum()!= max_disp_no*max_work_no){t.GetWorkers();cpl-=1;}
        }
        if(t.ChkRegStat()==true){cpl=cpl&1;} 
        else {t.ReRegister(); cpl=0;}
        if(cpl==1)break;  
    }
        	        
    while(1){
    	 new_task = NULL;  cerr << "Notice:Server is ready for incoming task!" << endl;
       master = fd_sets;
       if(select(fmax + 1, &master, NULL, NULL, NULL) < 0)       
       {
          cerr << "Fatal Error: select failure occurs!" << endl;
          exit(-1);        	
       }
          
       if(host.GetMachineType() == MASTER)
       {  
         t.HandlePipeInput(pipe_fd,&master);
       } 
          
       t.HandleIncomingReq(pipe_fd,&master);                                      //check incoming request or connection 
       
       if(host.GetMachineType()==MASTER)
          new_task = t.ProcessNewReq();
                               
       if(new_task)                                                               //Create a new process 
       {                                 
         child_pid = fork();
         if(child_pid < 0)
         {
           cerr << "Fatal Error: new task creation failed!" << endl;
           exit(-1);
         }         	
       
         if(child_pid == 0)                                                      //child process 
         {        
	         new_task->OpenStatFile(0); new_task->WriteMch(); new_task->WriteStat("Rcvd");	
         	 new_task->InitRequest(pipe_fd);          	 
         	 cerr << "Dispatcher process "<<new_task->GetTaskId()<<" is started!" << endl;         	 
         	 break;
         }          
       }       
    } 
        
    if(host.GetMachineType() == MASTER)                                
    {
      FD_SET(pipe_fd[new_task->GetTaskId()+max_disp_no][0],&fd_sets);             
      	if(fmax < pipe_fd[new_task->GetTaskId()+max_disp_no][0]) 
      		 fmax = pipe_fd[new_task->GetTaskId()+max_disp_no][0]; 
        new_task->SetFd(&fd_sets); new_task->SetFdmax(&fmax); 
          	
      while(1)
      {  	
        cerr << "Dispatcher process " << new_task->GetTaskId() 
             << " is ready for worker reply!" << endl;
      	master = fd_sets;
        select(fmax + 1, &master, NULL, NULL, NULL);  
        new_task->HandlePipeInput(pipe_fd,&master);         
        Rpl err = new_task->HandleIncomingReq(pipe_fd,&master); 
        if(err == CPL_OK) break;  
      }	      
    	new_task->HandlePipeOutput(pipe_fd,RLS_REQ,0);  
    	new_task->CloseStatFile();    
    	delete new_task;
    }
    
    return(1);
}
