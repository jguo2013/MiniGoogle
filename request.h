/**************************
   TinyGoogle project
   request.h
   author: J. Guo
   version: 1.0 /11/28/2013
   request format: index:  <loc>
                   search: <keyword num>-><word 1>-><word 2>->...
                   index  reply: <state>
                   search reply: <state>-><doc 1>-><word count>-><word>-><count>->...     
**************************/
#ifndef _request
#define _request

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <stdio.h>
#include <iostream>
#include <string>
#include <deque>
#include "connection.h"

using namespace std;

enum ReqType{INDEX_REQ, INDEX_REPLY, SEARCH_REQ, SEARCH_REPLY, PERMIT_REQ, PERMIT_REPLY, 
	           RLS_REQ, RLS_REPLY, WORKER_REQ, WORKER_REPLY, PERMIT_RLS_REQ, 
             REG_REQ, REG_REPLY, INDEX_MAP_REQ,INDEX_MAP_REPLY,INDEX_REDUCE_REQ, INDEX_REDUCE_REPLY,
             SEARCH_REDUCE_REQ,  SEARCH_REDUCE_REPLY, SERVER_FAIL_REPLY,INVALID_REQ_REPLY}; 
        
class Request {
   
private:
   deque<string>  data;  
   ReqType        req;
   Connection     conn; 

public:
   Request();
	 Request(Connection _c);

   void          ExtractWord(char *_b);
   void          SetMessage(ReqType _r, Connection _c,int _rsvd);  	     
   void          SetReqType(ReqType _t);
   void          SetConn(Connection _c); 

   int           GetRequest();       
   int           GetReqNum();
   int           GetReqOffset();
   deque<string> GetReqKey(); 
   deque<string> GetReqFile(); 
   Connection    GetConn();    
   ReqType       GetReqType();                     
   void          SendMessage();                    
   ostream & Print(ostream &os) const;
};
inline ostream & operator<<(ostream &os, const Request & _r) { return _r.Print(os);}                     
#endif
