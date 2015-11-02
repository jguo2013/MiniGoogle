/**************************
   TinyGoogle project
   request.cc
   author: 
   version: 1.0 /11/28/2013 
**************************/
#include <netdb.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/un.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/utsname.h>
#include <stdio.h>
#include <iostream>
#include <sstream>
#include <fstream>
#include <string>
#include "request.h"
#include "machine.h"
#include "global.h"
#define NAME "./tmp/index"

Request::Request(){}

Request::Request(Connection _c):conn(_c){}

void Request::SetReqType(ReqType _t)
{
  req = _t;	
}

void Request::SetConn(Connection _c)
{
	conn = _c;
}  

Connection Request::GetConn()
{
	return(conn);
}    
	
ReqType Request::GetReqType()
{
	return(req);
}  

int Request::GetRequest()
{
	char b[1024]; data.clear(); int len, qlen, f=1; 
  do
  {
     read(conn.GetSocket(),b,1024); len = strlen(b);
     if(len == 0){return(-1);}
     char *pch; string s; pch = strtok(b,","); s = pch; 
     if(f==1)
     {  //extract the number
        qlen = atoi(s.c_str()); qlen-= (s.size()+1); 
        pch = strtok (NULL, " ,"); f=0; 
     }
     while (pch != NULL && qlen > 0)
     {      
       s = pch; if(qlen<(s.size()+1)){s = s.substr(0,qlen); qlen=0;}
       else {qlen-=(s.size()+1); } data.push_back(s); 
       cerr << s << " " << qlen << endl;       
       pch = strtok(NULL, " ,\n"); 
     } 
  }while(qlen>0&&len!=0);
  
  deque<string>::iterator it = data.begin();f=0;
  
  if(strcmp((*it).c_str(),"register_reply") == 0)	
	{
	 req = REG_REPLY;f=1;
	}
  if(strcmp((*it).c_str(),"worker_reply") == 0)
	{
	 req = WORKER_REPLY;f=1;
	}
  if(strcmp((*it).c_str(),"index") == 0)
	{
	 req = INDEX_REQ;	f=1; 
	}
  if(strcmp((*it).c_str(),"index_map") == 0)
	{
	 req = INDEX_MAP_REQ;	f=1; 
	}	
  if(strcmp((*it).c_str(),"index_map_reply") == 0)
	{
	 req = INDEX_MAP_REPLY;f=1;	 
	}		
  if(strcmp((*it).c_str(),"index_reduce") == 0)
	{
	 req = INDEX_REDUCE_REQ;f=1;	 
	}		
  if(strcmp((*it).c_str(),"index_reduce_reply") == 0)
	{
	 req = INDEX_REDUCE_REPLY;f=1;	 
	}		
  if(strcmp((*it).c_str(),"search") == 0)
	{
	 req = SEARCH_REQ;f=1;	 
	}			
  if(strcmp((*it).c_str(),"search_reduce") == 0)
	{
	 req = SEARCH_REDUCE_REQ;	f=1; 
	}	
  if(strcmp((*it).c_str(),"search_reduce_reply") == 0)
	{
	 req = SEARCH_REDUCE_REPLY;	f=1; 
	}	
  if(strcmp((*it).c_str(),"permit_reply") == 0)
	{
	 req = PERMIT_REPLY;f=1;	 
	}	
  if(strcmp((*it).c_str(),"permit") == 0)
	{
	 req = PERMIT_REQ;f=1;	 
	}	
  if(strcmp((*it).c_str(),"permit_release") == 0)
	{
	 req = PERMIT_RLS_REQ;f=1;	 
	}			
  if(strcmp((*it).c_str(),"release") == 0)
	{
	 req = RLS_REQ;f=1;	 
	}
		
	if(f==1) {data.pop_front(); return(0);}
	else     {data.clear();    return(-1);}
}  

void Request::SendMessage()
{ 
	deque<string>::iterator it = data.begin();  
  switch(conn.GetTransType())
  {
   case TCP:
   	   {
   	   	int l = (*it).size();
   	   	char s[l+1];(*it).copy(s,l,0); s[l] = '\0';
   	    ssize_t len = write(conn.GetSocket(),s,(*it).size());
        if((ssize_t)(*it).size() != len)
        {
         cerr << "Fatal Error: message sending failed in SendMessage()!"<< endl;
         exit(-1);  		
        }
       }   break;
   case UDP:
       {
       	struct hostent  *hp;
        struct sockaddr_in remote;
        remote.sin_family=AF_INET;       	
       	hp = gethostbyname(conn.GetDest()->GetHostName());
        if (hp == NULL)
        {
          cerr << "Fatal Error: gethostbyname failed in SendMessage()!"<< endl;
          exit(-1);
        }        	
        bcopy ( hp->h_addr, &(remote.sin_addr.s_addr), hp->h_length);//set ip address
        remote.sin_port = htons(conn.GetDestPort());
        sendto(conn.GetSocket(),(*it).c_str(),strlen((*it).c_str())+1,0,(struct sockaddr *)&remote,sizeof(remote));
       }
   break;
  }
}
   
void Request::SetMessage(ReqType _r, Connection _c, int _rsvd)
{
	string s1,s2; char *c;
  s2.clear();
	data.clear();	  	
  switch(_r)
  {
   case REG_REQ: //<op>,<type>,<ip>,<port>,<machine attribute>
   	    switch(_c.GetSrc()->GetMachineType())
   	    {
   	    	case(WORKER): c = "worker,"; s1 = c; break;
   	    	case(MASTER): c = "master,"; s1 = c; break;		 
        }
        s2.insert(0,s1);
        s2.insert(s2.size(),_c.GetSrc()->GetIpAddr());
        s2.push_back(',');
   	    s1 = slib::_itoa(_c.GetSrc()->GetPortNum());
   	    s2.insert(s2.size(),s1);s2.push_back(',');
   	    s1 = _c.GetSrc()->GetHostName();
   	    s2.insert(s2.size(),s1);break;
   case WORKER_REQ: //<op>,<worker number>
   	    s1 = slib::_itoa(_c.GetSrc()->GetNum());      
   	    s2.insert(0,s1);break;
   case PERMIT_REQ: //<op>,<id>,<read/write>
   case PERMIT_RLS_REQ:   	
   case RLS_REQ:   	
   	    s1 = slib::_itoa(_rsvd);      
   	    s2.insert(0,s1);s2.push_back(',');   	    
   	    switch(_c.GetSrc()->GetTaskOp()){
          case REDUCE: s2.insert(s2.size(),"write"); break;
          case SEARCH: s2.insert(s2.size(),"read" ); break; 
        } break;                   	    
   case INDEX_MAP_REQ://<op>,<loc>,<offset>,<len>
   	    {
   	      deque<string>::iterator it = (*_c.GetDest()->GetFileName()).begin();       
   	      s1 = *it;
   	      s2.insert(0,s1); s2.push_back(',');
   	      s1 = slib::_itoa(_c.GetDest()->GetOffset());
   	      s2.insert(s2.size(),s1);s2.push_back(',');
   	      s1 = slib::_itoa(_c.GetDest()->GetNum());
   	      s2.insert(s2.size(),s1);break; 
   	    }  	            	    
   case INDEX_REDUCE_REQ://<op>,<offset>,<num>,<host name>
   	    {      	    
         s1 = slib::_itoa(_c.GetDest()->GetOffset());       
   	     s2.insert(0,s1);         s2.push_back(',');
   	     s1 = slib::_itoa(_c.GetDest()->GetNum());
   	     s2.insert(s2.size(),s1); 
   	     deque<string> *q = _c.GetSrc()->GetKeys();
   	     for(deque<string>::iterator it=(*q).begin();it!=(*q).end();it++)
   	     {s2.push_back(','); s2.insert(s2.size(),*it);}
   	    }break;    	
   case INDEX_MAP_REPLY://<op>,<state>   	    
   case INDEX_REDUCE_REPLY:
   	    switch(_c.GetSrc()->GetMachineState())
   	    {
   	    	case(WORKER): s1 = "complete"; break;
   	    	case(MASTER): s1 = "error"; break;		 
        } s2.insert(0,s1);break;   	
   case SEARCH_REDUCE_REPLY://<op>,<state>,<number>,<doc>,<word>,...... 
   	    {
   	    switch(_c.GetSrc()->GetMachineState())
   	    {
   	    	case(WORKER): s1 = "complete,"; break;
   	    	case(MASTER): s1 = "error,"; break;
   	    }
   	    s2.insert(0,s1);			
   	    s1 = slib::_itoa(_c.GetSrc()->GetNum()); 
   	    s2.insert(s2.size(),s1);    	    		
   	    deque<string> *q = _c.GetSrc()->GetKeys(); 
   	    for(deque<string>::iterator it=(*q).begin();it!=(*q).end();it++)
   	    	 {s2.push_back(','); s2.insert(s2.size(),*it);}	 
   	    }  break;   	  	     	
   case SEARCH_REDUCE_REQ:  //<op>,<num>,<word 1>,<word 2>,...... 
   	    {
   	    s1 = slib::_itoa(_c.GetDest()->GetNum()); 
   	    s2.insert(0,s1);  	    		
   	    deque<string> *q = _c.GetDest()->GetKeys();
   	    for(deque<string>::iterator it=(*q).begin();it!=(*q).end();it++)
   	    	 {s2.push_back(','); s2.insert(s2.size(),*it);}
   	    }	 break; 
   case SEARCH_REPLY:
        {
         deque<string> *q = _c.GetSrc()->GetKeys(); 
         int n = (*q).size()/2;  s2.insert(0,slib::_itoa(n));      
   	     for(deque<string>::iterator it=(*q).begin();it!=(*q).end();it++)
   	    	 {s2.push_back(','); s2.insert(s2.size(),*it);}         
        }  break; 	
   case INDEX_REPLY://<op>,<succeed>/<fail> 
   	    s1 = "succeed"; s2.insert(0,s1);
   	    break;	   	        	  	 	
   default:;  
  }
 
  switch(req)
  {
   case REG_REQ:             s1 = ",register,";            break;
   case WORKER_REQ:          s1 = ",worker_req,";          break;
   case INDEX_REPLY:         s1 = ",index_reply,";         break;   	
   case INDEX_MAP_REQ:       s1 = ",index_map,";           break;   	
   case INDEX_MAP_REPLY:     s1 = ",index_map_reply,";     break;   	
   case INDEX_REDUCE_REQ:    s1 = ",index_reduce,";        break;   	
   case INDEX_REDUCE_REPLY:  s1 = ",index_reduce_reply,";  break;   	
   case SEARCH_REDUCE_REQ:   s1 = ",search_reduce,";       break;   	
   case SEARCH_REDUCE_REPLY: s1 = ",search_reduce_reply,"; break;
   case SEARCH_REPLY:        s1 = ",search_reply,";        break;   	   	
   case PERMIT_REQ:          s1 = ",permit,";              break;
   case PERMIT_RLS_REQ:      s1 = ",permit_release,";      break;   	  	
   case PERMIT_REPLY:        s1 = ",permit_reply";         break;   	   	
   case RLS_REQ:             s1 = ",release,";             break;   	
   default:;  
  }

  s2.insert(0,s1); s1=slib::_itoa(s2.size()+10);           //spare 10 bytes for total number field  
  s2.insert(0,s1); int i=s1.size();
  while(i<10){s2.insert(0," ");i++;}                       //filled with space if not 10 bytes    
  data.push_back(s2);      	  	
} 

int Request::GetReqNum()
{
	int n=0;
  switch(req)
  {
   case WORKER_REPLY://<worker number>,<worker 1 ip>,<worker 1 port>,<worker 1 attr>,<worker 2 attr> 
   	    n = atoi(data.at(0).c_str()); break;
   case INDEX_MAP_REQ://<loc>,<offset>,<num>	 
	      n = atoi(data.at(2).c_str()); break;
	 case INDEX_REDUCE_REQ://<offset>,<num>,<other host name>	 
	      n = atoi(data.at(1).c_str()); break;
   case INDEX_REDUCE_REPLY:break;//<state>   	    	 
   case INDEX_MAP_REPLY:break;//<state>
	 case SEARCH_REQ://<number>,<key 1>,<key 2>,...	 
	      n = atoi(data.at(0).c_str());break;	 
   case SEARCH_REDUCE_REQ://<number>,<key 1>,<key 2>,...	 
   	    n = atoi(data.at(0).c_str());break;	 
   case SEARCH_REDUCE_REPLY://<number>,<doc 1>,<count 1>,...
	      n = atoi(data.at(0).c_str());break;
   case PERMIT_REQ:
   case PERMIT_RLS_REQ:   	
   case RLS_REQ: //<id><write/read>	 
	      n = atoi(data.at(0).c_str());break;
   default:;  
  }	
  return(n);
}
int Request::GetReqOffset()
{
	int n=0;
  switch(req)
  {
   case INDEX_MAP_REQ://<loc>,<offset>,<num>	 
	      n = atoi(data.at(1).c_str()); break;
	 case INDEX_REDUCE_REQ://<offset>,<num>	 
	      n = atoi(data.at(0).c_str()); break;  
   default:;  
  }	
  return(n);
}

deque<string> Request::GetReqKey()
{
	deque<string>::iterator it = data.begin();
	deque<string> q; q.clear();
  switch(req)
  {
   case WORKER_REPLY://<worker number>,<worker 1 port>,<worker 1 attr>,<worker 2 attr> 
   case SEARCH_REDUCE_REQ://<number>,<key 1>,<key 2>,...	 
	 case SEARCH_REQ://<number>,<key 1>,<key 2>,...	
   case PERMIT_REQ://<id><read/write>	
   case PERMIT_RLS_REQ: 
   case RLS_REQ:   //<id><read/write>	 	 
	 	    it++;  break;
   case SEARCH_REDUCE_REPLY://<complete>,<number>,<doc 1>,<count 1>,...
	 case INDEX_REDUCE_REQ://<offset>,<num>,<host name>,<host name>,....	
	 	    it+=2; break; 
   case INDEX_MAP_REQ://<loc>,<offset>,<num>	 
   case INDEX_REDUCE_REPLY://<state>   	    	 
   case INDEX_MAP_REPLY://<state>
	      it = data.end();break;   		 
   default:;  
  }	
	for(; it != data.end();it++)q.push_back(*it);
  return(q);	
} 
   
deque<string> Request::GetReqFile()
{
	deque<string>::iterator it;
	deque<string> q; q.clear();
	  
  switch(req)
  {
   case INDEX_REQ://<loc>
   case INDEX_MAP_REQ://<loc>,<offset>,<num>
   case PERMIT_REQ:	
   case PERMIT_RLS_REQ:
   case PERMIT_REPLY: 
   case RLS_REQ:    	  	 
	      it = data.begin(); break;
	 default://<offset>,<num>	 
	      it = data.end(); break;  
  }	  
  if(it != data.end()) q.push_back(*it);
  	
  return(q);	
}

ostream & Request::Print(ostream &os) const
{
	 deque<string>::const_iterator it;
   os << "Message(Request type=" << req <<" \n"
      << "        message: "     << " \n"
      << "        ";
  
   for(it = data.begin(); it != data.end();it++)
   { 
      os <<(*it)<<"->";   	  	      
   }  os << " \n" << "        )" << " \n"; 
   return os;
}
