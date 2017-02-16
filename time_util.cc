/* Copyright 2016 The TensorFlow Authors. All Rights Reserved.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
   ==============================================================================*/

#include "tensorflow/core/platform/s3cloud/time_util.h"
#include <time.h>
#include <cmath>
#include <cstdio>
#include <ctime>
#include "tensorflow/core/lib/core/errors.h"
#include <math.h>
#include <openssl/hmac.h>
#include <openssl/evp.h>    
namespace tensorflow {
namespace S3 {
  namespace {
    constexpr int64 kNanosecondsPerSecond = 1000 * 1000 * 1000;

  }  // namespace

  // Only implements one special case of RFC 3339 which is returned by
  // GCS API, e.g 2016-04-29T23:15:24.896Z.
  Status ParseRfc3339Time(const string& time, int64* mtime_nsec) {
    tm parsed{0};
    float seconds;
    if (sscanf(time.c_str(), "%4d-%2d-%2dT%2d:%2d:%fZ", &(parsed.tm_year),
               &(parsed.tm_mon), &(parsed.tm_mday), &(parsed.tm_hour),
               &(parsed.tm_min), &seconds) != 6) {
      return errors::Internal(
                              strings::StrCat("Unrecognized RFC 3339 time format: ", time));
    }
    const int int_seconds = floor(seconds);
    parsed.tm_year -= 1900;  // tm_year expects years since 1900.
    parsed.tm_mon -= 1;      // month is zero-based.
    parsed.tm_sec = int_seconds;

    *mtime_nsec = timegm(&parsed) * kNanosecondsPerSecond +
      floor((seconds - int_seconds) * kNanosecondsPerSecond);

    return Status::OK();
  }
  Status GetRfc850Time(time_t time,string&timeStr){
    char buf[64]={0};
    struct tm result;
    memset(&result,0,sizeof(result));
    struct tm* tmRet = gmtime_r(&time,&result);
    if(tmRet == NULL){
      return errors::Internal
        (strings::StrCat("gmtime_r :", time,
                         " failure due to error:",strerror(errno)));
    }
    size_t size = 
      strftime(buf, sizeof(buf), "%a, %d %b %Y %H:%M:%S GMT", &result);
    
    if(size == 0){
      return errors::Internal
        (strings::StrCat("strftime failure due to return 0"));
    }
    timeStr = string(buf);
    return Status::OK();
  }
  
  Status ParseRfc850Time(const string&datestr,time_t &time_sec)
  {
    struct tm tp;
    memset(&tp,0,sizeof(tp));
    if(NULL == strptime(datestr.c_str(),"%a, %d %b %Y %H:%M:%S GMT",&tp)){
      return errors::Internal
        (strings::StrCat("ParseRfc850Time failure for str:",datestr));
    }
    time_sec = timegm(&tp);
    return Status::OK();
  }
  
  int Base64Encode(const char *encoded, int encodedLength, char *decoded){
    
    return EVP_EncodeBlock((unsigned char*)decoded, 
                           (const unsigned char*)encoded, encodedLength);  
  }  
  
  Status hmac_sha1_raw(const void* key, size_t keylen, 
                     const unsigned char* data, size_t datalen, 
                     unsigned char** digest, unsigned int* digestlen){
    if(!key || 0 >= keylen || !data || 0 >= datalen){
      return errors::Internal
        (strings::StrCat("hmac_sha1_raw check failure key:",(const char*)key,
                         " keylen:",keylen," data:",(const char*)data,
                         " datalen:",datalen));
    }
    (*digestlen) = EVP_MAX_MD_SIZE * sizeof(unsigned char);
    if(NULL == ((*digest) = (unsigned char*)malloc(*digestlen))){
      return errors::Internal
        (strings::StrCat("hmac_sha1_raw malloc :",*digestlen,
                         " failure due to error:",strerror(errno)));
    }
    if(NULL==HMAC(EVP_sha1(), key, keylen, data, datalen, *digest, digestlen)){
      return errors::Internal
        (strings::StrCat("HMAC failure key:",(const char*)key,
                         " keylen:",keylen," data:",(const char*)data,
                         " datalen:",datalen));
    }
    return Status::OK();
  }
  
  Status hmac_sha1(const string &keySecret, 
                   const string &stringToSign, 
                   string &out){
    unsigned char* outbuf = NULL;
    unsigned int outlen = 0;
    Status st = hmac_sha1_raw(keySecret.c_str(),keySecret.size(),
                              (const unsigned char*)stringToSign.c_str(),
                              stringToSign.size(),
                              &outbuf,&outlen);
    if(st != Status::OK()){
      free(outbuf);
      return st;
    }
    char out_based64[256] = {0};
    Base64Encode((const char *)outbuf,outlen,(char*)out_based64);
    out = out_based64 ;
    free(outbuf);
    return Status::OK();
  }
  
  Status ComputeS3Signed(const string&s3secret,
                         const string&toSign,
                         string&signedValue){
    return hmac_sha1(s3secret,toSign,signedValue);
  }

}

}  // namespace tensorflow
