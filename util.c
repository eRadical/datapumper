#include<stdlib.h> 
#include<string.h>
#include<util.h>

int cmp(const void *p1, const void *p2){
    return strcmp(* (char * const *) p1, * (char * const *) p2); 
}


char *strnmov(register char *dst, register const char *src, size_t n)
{
  while (n-- != 0) {
    if (!(*dst++ = *src++)) {
      return (char*) dst-1;
    }
  }
  return dst;
}

char *strmake(register char *dst, register const char *src, size_t length)
 {
   while (length--)
     if (! (*dst++ = *src++))
       return dst-1;
   *dst=0;
   return dst;
 }



