/**
 * Returns the current date
 * in a format suitable for a HTTP request header.
 */
#include <assert.h>
#include <string>
#include <ctime>
#include <stdio.h>
#include <string.h>
#include <math.h>
#include <openssl/hmac.h>
#include <openssl/evp.h>  
using namespace std;
long kNanosecondsPerSecond = 1000 * 1000 * 1000;
string get_date_rfc850()
{
  char buf[100];
  time_t t = time(NULL);
  strftime(buf, sizeof(buf), "%a, %d %b %Y %H:%M:%S GMT", gmtime(&t));
  return buf;
}
string get_date_rfc850(time_t time)
{
  char buf[100];
  strftime(buf, sizeof(buf), "%a, %d %b %Y %H:%M:%S GMT", gmtime(&time));
  return buf;
}
//Last-Modified: Thu, 09 Feb 2017 09:44:08 GMT

/*extern char*strptime (__const char *__restrict __s,  
__const char *__restrict __fmt,struct tm *__tp)  
__THROW;  */
int parse_date_rfc850(const char*datestr,time_t *time_sec)
{
  struct tm tp;
  memset(&tp,0,sizeof(tp));
  strptime(datestr,"%a, %d %b %Y %H:%M:%S GMT",&tp);
  *time_sec = timegm(&tp);
  return 0;
}


//// Only implements one special case of RFC 3339 which is returned by
// GCS API, e.g 2016-04-29T23:15:24.896Z.
// S3 API       2017-02-09T09:44:08.555Z
int ParseRfc3339Time(const string& time, long* mtime_nsec) {
  tm parsed{0};
  float seconds;
  if (sscanf(time.c_str(), "%4d-%2d-%2dT%2d:%2d:%fZ", &(parsed.tm_year),
             &(parsed.tm_mon), &(parsed.tm_mday), &(parsed.tm_hour),
             &(parsed.tm_min), &seconds) != 6) {
    return -1;
  }
  const int int_seconds = floor(seconds);
  parsed.tm_year -= 1900;  // tm_year expects years since 1900.
  parsed.tm_mon -= 1;      // month is zero-based.
  parsed.tm_sec = int_seconds;

  *mtime_nsec = timegm(&parsed) * kNanosecondsPerSecond +
    floor((seconds - int_seconds) * kNanosecondsPerSecond);

  return 0;
}                                 

const  string s3Key="XN6QBMRYUE5H49KNVT1D"  ;
const  string s3Secret="bo7ISjXTyrKElDkzLAsBCvXDvN22xh6e9VUA6qFS"  ;
const  string stringToSign="HEAD\n\napplication/x-compressed-tar\nThu, 09 Feb 2017 11:50:49 +0000\n/BUCKET-mygod/testfile";

int Base64Encode(const char *encoded, int encodedLength, char *decoded)  
{      
    return EVP_EncodeBlock((unsigned char*)decoded, (const unsigned char*)encoded, encodedLength);  
}  
  
// base解码  
int Base64Decode(const char *encoded, int encodedLength, char *decoded)   
{      
    return EVP_DecodeBlock((unsigned char*)decoded, (const unsigned char*)encoded, encodedLength);  
}  
bool hmac_sha1_raw(const void* key, size_t keylen, 
               const unsigned char* data, size_t datalen, 
               unsigned char** digest, unsigned int* digestlen)
{
  if(!key || 0 >= keylen || !data || 0 >= datalen || !digest || !digestlen){
    return false;
  }
  (*digestlen) = EVP_MAX_MD_SIZE * sizeof(unsigned char);
  if(NULL == ((*digest) = (unsigned char*)malloc(*digestlen))){
    return false;
  }
  unsigned char * ret = 
    HMAC(EVP_sha1(), key, keylen, data, datalen, *digest, digestlen);
  
  return true;
}
bool hmac_sha1(const string &keySecret,  //hello
               const string &stringToSign,  //hahaha
               string &out){
  unsigned char* outbuf = NULL;
  unsigned int outlen = 0;
  bool ret = hmac_sha1_raw(keySecret.c_str(),keySecret.size(),
                           (const unsigned char*)stringToSign.c_str(),
                           stringToSign.size(),
                           &outbuf,&outlen);
  char out_based64[1024] = {0};
  Base64Encode((const char *)outbuf,outlen,(char*)out_based64);
  out = out_based64 ;
  free(outbuf);
  return true;
}
int auth_test(int argc,char**argv){/*
  time_t now = 1486693455;//Fri, 10 Feb 2017 02:24:15 GMT  
  string now_str = get_date_rfc850(now);
  time_t now_parsed = 0 ;
  parse_date_rfc850(now_str.c_str(),&now_parsed);
  printf("now:%s\n",now_str.c_str());
  printf("now:%lu now_parsed:%lu\n",now,now_parsed);
  long parsed3339 = 0;
  ParseRfc3339Time("2017-02-10T02:24:15.555Z",&parsed3339);
  printf("parsed3339:%ld\n",parsed3339);
  unsigned char  *outbuf= new unsigned char[1024];
  memset(outbuf,0,1024);
  unsigned int outlen = sizeof(outbuf);
  bool ret = hmac_sha1_raw(s3Secret.c_str(),s3Secret.size(),(const unsigned char*)stringToSign.c_str(),stringToSign.size(),&outbuf,&outlen);
  printf("ret:%d, len:%d, out:%s\n",ret,outlen,outbuf);
  
  char out[1024] = {0};
  Base64Encode((const char *)outbuf,outlen,(char*)out);
  printf("%s\n",out); 
  string outauth;
  hmac_sha1(s3Secret,stringToSign,outauth);
  printf("%s\n",outauth.c_str());
                    */
  string outauth;
  hmac_sha1("XN6QBMRYUE5H49KNVT1D",
            "GET",outauth);
  printf("%s\n",outauth.c_str());

  //right   fjbMY7nr6Jp9wse0i3VEeOTWaWU=
  //output  fjbMY7nr6Jp9wse0i3VEeOTWaWU=
  
}
#include "tinyxml2.h"
using namespace tinyxml2;
int xml_test(int argc,char** argv){
  static const char* xml =
    "<?xml version=\"1.0\"?>"
    "<!DOCTYPE PLAY SYSTEM \"play.dtd\">"
    "<PLAY>"
    "<TITLE>A Midsummer Night's Dream</TITLE>"
    "</PLAY>";

  XMLDocument doc;
  doc.Parse( xml );

  XMLElement* titleElement = doc.FirstChildElement( "PLAY" )->FirstChildElement( "TITLE" );
  const char* title = titleElement->GetText();
  printf( "Name of play (1): %s\n", title );

  XMLText* textNode = titleElement->FirstChild()->ToText();
  title = textNode->Value();
  printf( "Name of play (2): %s\n", title );

  printf("errorid:%d\n",doc.ErrorID());
}

#include <curl/curl.h>
size_t write_callback(char *ptr, size_t size, size_t nmemb, void *userdata){
  printf("write_callback:size:%ld nmenb:%ld userdata:%p data:%s\n",
         size,nmemb,userdata,ptr);
}
size_t read_callback(char *buffer, size_t size, size_t nitems, void *instream){
  return 0;
}
int curl_test(int argc,char** argv){
  CURL * curl = curl_easy_init();
  assert(curl!=NULL);
  curl_easy_setopt(curl,CURLOPT_URL,"http://www.baidu.com");
  curl_easy_setopt(curl, CURLOPT_NOSIGNAL,1L);
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION,write_callback);
  //CURLcode curl_easy_setopt(CURL *handle, CURLOPT_WRITEDATA, void *pointer);
  curl_easy_setopt(curl, CURLOPT_WRITEDATA,0);  
  curl_easy_setopt(curl, CURLOPT_HTTP_VERSION,CURL_HTTP_VERSION_1_1);
  curl_easy_setopt(curl, CURLOPT_RANGE,"0-10000");
  curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "GET");
  curl_easy_setopt(curl, CURLOPT_PUT, 1);
  curl_easy_setopt(curl, CURLOPT_POST, 1);
  curl_easy_setopt(curl, CURLOPT_HTTPGET, 1);
  curl_easy_setopt(curl, CURLOPT_READFUNCTION,read_callback);
  curl_easy_setopt(curl, CURLOPT_READDATA,0);
  curl_easy_setopt(curl, CURLOPT_VERBOSE,1);
  curl_easy_perform(curl);
  curl_easy_cleanup(curl);
}
int main(int argc,char**argv){
  //xml_test(argc,argv);
  auth_test(argc,argv);
  //curl_test(argc,argv);
  return 0;
}
