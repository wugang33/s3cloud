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

#ifndef TENSORFLOW_CORE_PLATFORM_AUTH_PROVIDER_H_
#define TENSORFLOW_CORE_PLATFORM_AUTH_PROVIDER_H_

#include <string>
#include "tensorflow/core/lib/core/errors.h"
#include "tensorflow/core/lib/core/status.h"

namespace tensorflow {
namespace S3 {
/// Interface for a provider of authentication bearer tokens.
class AuthProvider {
 public:
  virtual ~AuthProvider() {}

  virtual Status GetHost(string*host) = 0;
  virtual Status GetKey(string*key) = 0;
  virtual Status GetSecret(string*seceret) = 0;
  
  static Status GetHost(AuthProvider* provider, string* token) {
    if (!provider) {
      return errors::Internal("Auth provider is required.");
    }
    return provider->GetHost(token);
  }
  static Status GetKey(AuthProvider* provider, string* token) {
    if (!provider) {
      return errors::Internal("Auth provider is required.");
    }
    return provider->GetKey(token);
  }
  static Status GetSecret(AuthProvider* provider, string* token) {
    if (!provider) {
      return errors::Internal("Auth provider is required.");
    }
    return provider->GetSecret(token);
  }
};
}
}  // namespace tensorflow

#endif  // TENSORFLOW_CORE_PLATFORM_AUTH_PROVIDER_H_
