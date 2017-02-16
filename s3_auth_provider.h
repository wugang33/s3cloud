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

#ifndef TENSORFLOW_CORE_PLATFORM_GOOGLE_AUTH_PROVIDER_H_
#define TENSORFLOW_CORE_PLATFORM_GOOGLE_AUTH_PROVIDER_H_

#include <memory>
#include "tensorflow/core/platform/s3cloud/auth_provider.h"

namespace tensorflow {
namespace S3 {
/// Implementation based on S3 Application Default Credentials.
class S3AuthProvider : public AuthProvider {
 public:
  S3AuthProvider();
  virtual ~S3AuthProvider() {}
  
  Status GetHost(string* token) override;
  Status GetKey(string* token) override;
  Status GetSecret(string* token) override;

 private:
  Status ParseS3Cfg();
  string host_;
  string key_;
  string secret_;
  TF_DISALLOW_COPY_AND_ASSIGN(S3AuthProvider);
};
}
}  // namespace tensorflow

#endif  // TENSORFLOW_CORE_PLATFORM_GOOGLE_AUTH_PROVIDER_H_
