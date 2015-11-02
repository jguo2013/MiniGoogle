#ifndef _global
#define _global

#include <iostream>
#include <sstream>
#include <string>
#include <stdio.h>
#include <stdlib.h>

namespace slib
{
   inline string _itoa(unsigned int n)
   {
    ostringstream ostr; ostr << n;
    return(ostr.str());
   } 
 
   inline int getlen(int _o, int _div, int _t)
   {  
     	int len = _o/_div;
     	if(_t == _div-1) len = _o - len*_t;         
  	  return(len);
   } 
   
   inline int getoffset(int _o, int _s, int _div, int _t)
   {  
     	return(_o + _s/_div*_t);          
   }  
   
   inline deque<string> getkey(deque<string> *_s, int _div, int _t)
   {  
   	  deque<string> s; int i=0;
   	  for(deque<string>::iterator it = (*_s).begin();it != (*_s).end();it++)
   	  {
   	  	 if(i%_div==_t) s.push_back(*it);i++;
   	  } 
     	return(s);          
   }
}
#endif
