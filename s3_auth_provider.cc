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

#include "tensorflow/core/platform/s3cloud/s3_auth_provider.h"
#include <pwd.h>
#include <sys/types.h>
#include <unistd.h>
#include <fstream>
#include "tensorflow/core/lib/core/errors.h"
#include "tensorflow/core/lib/io/path.h"
#include "tensorflow/core/lib/strings/base64.h"
#include "tensorflow/core/platform/env.h"
#include "tensorflow/core/platform/s3cloud/ini.h"
namespace tensorflow {
	namespace S3 {
  S3AuthProvider::S3AuthProvider(){}


  Status S3AuthProvider::GetHost(string* t) {
    if(host_.empty()){
      TF_RETURN_IF_ERROR(ParseS3Cfg());
    }
    *t = host_;
    return Status::OK();
  }

  Status S3AuthProvider::GetKey(string* t) {
    if(host_.empty()){
      TF_RETURN_IF_ERROR(ParseS3Cfg());
    }
    *t = key_;
    return Status::OK();
  }

  Status S3AuthProvider::GetSecret(string* t) {
    if(host_.empty()){
      TF_RETURN_IF_ERROR(ParseS3Cfg());
    }
    *t = secret_;
    return Status::OK();
  }
#include <unistd.h>
#include <sys/types.h>
#include <pwd.h>
    
  Status S3AuthProvider::ParseS3Cfg(){
    struct passwd *pw = getpwuid(getuid());
    const char *result = pw->pw_dir;
    if (!result) {
      return errors::NotFound
        (strings::StrCat("$HOME is not set"));
    }
    string filename = strings::StrCat(result,"/.s3cfg");
    std::ifstream fs(filename.c_str());
    if(false == fs.good()){
      return errors::NotFound
        (strings::StrCat(filename," is not found or corrput"));
    }
    try{
      
      INI::Parser p(fs);
      /*
      std::stringstream out;
      p.dump(out);
      printf("s3cfg contents:\n%s\n",out.str().c_str());
      */
      host_ = p.top()("default")["host_base"];
      key_ = p.top()("default")["access_key"];
      secret_ = p.top()("default")["secret_key"];
    } catch (std::runtime_error& e) {
      host_ = "";
      key_ = "";
      secret_ = "";
      return errors::DataLoss
        (strings::StrCat(e.what()," when parse ",filename));
    }
    if(host_.empty() || key_.empty() || secret_.empty()){
      string host_tmp = host_;
      string key_tmp = key_;
      string secret_tmp = secret_;
      host_ = "";
      key_ = "";
      secret_ = "";
      return errors::DataLoss
        (strings::StrCat("host:[",host_tmp,"] key:[",key_tmp,
                         "] secret:[",secret_tmp
                         ,"] can't be empty when parse file:",filename));
    }
    return Status::OK();
  }
	}   
}  // namespace tensorflow
