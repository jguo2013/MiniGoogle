/**************************
   TinyGoogle project
   connection.cc
   author: Jie Guo
   version: 1.0 /11/26/2013 
**************************/
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <fstream>
#include <string>
#include <netdb.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include "connection.h"
#include "machine.h"
#include "global.h"
using namespace std;

Connection::Connection():
sock(0),src_port(0),dest_port(0),src(0),dest(0){}

Connection::Connection(Machine * _src):src(_src),dest(0){}   

Connection::Connection(Machine * _src, Machine * _dest, SockType _s, TransType _t):
src(_src),dest(_dest), type(_s), ttype(_t),src_port(0),dest_port(0){} 

Connection::Connection(Machine * _src,int _s):src(_src),dest(0),sock(_s){}   
	
void Connection::ConSetup(fd_set * sets, int * _fmax, int flag)
{
	switch(ttype)
	  {
	   case TCP: sock = socket(AF_INET,SOCK_STREAM,0); break;
	   case UDP: sock = socket(AF_INET,SOCK_DGRAM,IPPROTO_UDP);  break;
    }
  struct sockaddr_in local;
  socklen_t len=sizeof(local);

  if(flag == 0){
     local.sin_family=AF_INET;
     local.sin_addr.s_addr=INADDR_ANY;
     local.sin_port=0;
     bind(sock,(struct sockaddr *)&local,sizeof(local));
     
     getsockname(sock,(struct sockaddr *)&local,&len);
     src_port = ntohs(local.sin_port);
  } 
  cerr <<"A new connection is setup... \n" 
       <<"src machine:" << src->GetHostName() <<"\n"
       <<"src port: " << src_port <<" \n\n"; 	    
  FD_SET(sock, sets);            	
  if (sock > *_fmax) *_fmax = sock; 
}

void Connection::ConListen()
{
  listen(sock,32); src->SetPortNum(src_port);
  cerr <<"Src machine " << src->GetHostName() 
       <<" is listening at port " << src->GetPortNum() <<" \n\n"; 	        
}
	
void Connection::ConAccept(fd_set * sets, int *fdmax, int _s)
{
	sock = accept(_s, 0,0);
	if(sock < 0){
	   cerr << "Fatal Error: accept fails! in ConAccept()" << endl;
	   exit(-1);
	}
  cerr << "A new connection "<< sock << " is ACCEPTED!" << endl;
  FD_SET(sock, sets); if (sock > *fdmax) *fdmax = sock;  	
}

void Connection::ConConnect()
{
  struct sockaddr_in remote;
  struct hostent *hp;//,*gethostbyname();

  remote.sin_family=AF_INET;
  hp=gethostbyname(dest->GetHostName());
  bcopy(hp->h_addr,(char*)&remote.sin_addr,hp->h_length);
  remote.sin_port= htons(dest_port);

  connect(sock,(struct sockaddr *)&remote,sizeof(remote));
  cerr << "A active connection is sent out"<<endl;

}
   
void Connection::ConClose(fd_set * sets, int *fdmax)
{
	close(sock);
}
   
int Connection::GetSocket()
{
	return(sock);
}

Machine * Connection::GetSrc()
{
	return(src);
}

Machine * Connection::GetDest()
{
	return(dest);
}

TransType  Connection::GetTransType()
{
 return(ttype);
}

SockType  Connection::GetSockType()
{
return(type);
}

void  Connection::SetDestPort(unsigned _n)
{
  dest_port = _n;
}

unsigned  Connection::GetDestPort()
{
	return(dest_port);
} 

void Connection::SetSrc(Machine * _m)
{
	src  = _m;
}

void Connection::SetDest(Machine * _m)
{
	dest = _m;
}
   
ostream & Connection::Print(ostream &os) const
{
  os << "Connection(sock=" <<sock<<", type="<<type<<", ttype="<<ttype<<" \n"
     << "           src= " <<src->GetHostName() <<" \n"
     << "           dest=" <<dest->GetHostName()<<" \n"
     << "           src  port=" <<src_port  <<" \n"
     << "           dest port=" <<dest_port <<" \n";     
  return os;
}
