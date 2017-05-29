#include<stdio.h>
#include<stdlib.h>
#include<my_global.h>

size_t dirname_length(const char *name)
 {
   register char *pos, *gpos;
    pos=(char*) name-1;

   gpos= pos++;
   for ( ; *pos ; pos++)
   {
     if (*pos == FN_LIBCHAR || *pos == '/')
       gpos=pos;
   }
   return (size_t) (gpos+1-(char*) name);
 }
