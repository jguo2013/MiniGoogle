#ifndef _connection
#define _connection

#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <iostream>
#include <string>
#include <stdio.h>
#include <string.h>
class Machine;
using namespace std;

enum TransType{TCP, UDP};
enum SockType{LISTEN_CONNECTION, ACTIVE_CONNECTION,CLIENT_CONNECTION};
             
class Connection {
   
protected:
   int       sock;
   SockType  type;
   TransType ttype;
   Machine * src;   
   Machine * dest;
   unsigned  src_port;                  
   unsigned  dest_port;
      
public:
   Connection();
   Connection(Machine * _src);  
   Connection(Machine * _src,int _s);     
   Connection(Machine * _src, Machine * _dest, SockType _s, TransType _t); 

   void ConConnect();     
   void ConListen();       
   void ConSetup(fd_set * sets, int *fdmax, int flag);       
   void ConClose(fd_set * sets, int * _fmax);
   void ConAccept(fd_set * sets, int *fdmax, int _s); 
      
   int       GetSocket();
   unsigned  GetDestPort();    
   Machine * GetSrc();
   Machine * GetDest();    
   SockType  GetSockType();   
   TransType GetTransType();
   void      SetSrc(Machine * _m);
   void      SetDest(Machine * _m);      
   void      SetDestPort(unsigned _n);          
   ostream & Print(ostream &os) const ;
};
inline ostream & operator<<(ostream &os, const Connection & _c) { return _c.Print(os);}

#endif
