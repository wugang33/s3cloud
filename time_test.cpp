/**
 * Returns the current date
 * in a format suitable for a HTTP request header.
 */
#include <string>
#include <ctime>
#include <stdio.h>
using namespace std;
string get_date_rfc850()
{
  char buf[100];
  time_t t = time(NULL);
  strftime(buf, sizeof(buf), "%a, %d %b %Y %H:%M:%S GMT", gmtime(&t));
  return buf;
}




int main(int argc,char**argv){
  string now = get_date_rfc850();
  printf("now:%s\n",now.c_str());
  return 0;
}
