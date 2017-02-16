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
#include "tensorflow/core/lib/core/status_test_util.h"
#include "tensorflow/core/platform/test.h"

namespace tensorflow {
  namespace S3{
    TEST(TimeUtil, ParseRfc3339Time) {
      int64 mtime_nsec;
      TF_EXPECT_OK(ParseRfc3339Time("2016-04-29T23:15:24.896Z", &mtime_nsec));
      // Compare milliseconds instead of nanoseconds.
      EXPECT_EQ(1461971724896, mtime_nsec / 1000 / 1000);
    }
    TEST(TimeUtil, ParseRfc850Time) {
      time_t mtime_nsec;
      TF_EXPECT_OK(ParseRfc850Time("Thu, 16 Feb 2017 01:17:42 GMT", 
                                   mtime_nsec));
      // Compare milliseconds instead of nanoseconds.
      EXPECT_EQ(1487207862l, mtime_nsec);
    }
    TEST(TimeUtil, ParseRfc850TimeError) {
      time_t mtime_nsec;
      EXPECT_EQ("ParseRfc850Time failure for str:Thu, 16 Feb 2017",
                ParseRfc850Time("Thu, 16 Feb 2017", 
                                mtime_nsec).error_message());
    }

    TEST(TimeUtil, GetRfc850Time) {
      time_t now = 1487207862l;// time(0);
      string timeStr ;
      TF_EXPECT_OK(GetRfc850Time(now,timeStr));
      EXPECT_EQ(timeStr,"Thu, 16 Feb 2017 01:17:42 GMT");
    }
    
    
    TEST(TimeUtil, ParseRfc3339Time_ParseError) {
      int64 mtime_nsec;
      EXPECT_EQ("Unrecognized RFC 3339 time format: 2016-04-29",
                ParseRfc3339Time("2016-04-29", &mtime_nsec).error_message());
    }
  
    TEST(TimeUtil, ComputeS3Signed) {
      string s3secret = "hello";
      string toSign = "hahaha";
      string signedValue;
      EXPECT_EQ(ComputeS3Signed(s3secret,toSign,signedValue),Status::OK());
      EXPECT_EQ(signedValue,"LXrsCWcskH/tJCFm7zw+TOP2R64=");
    }
  }

}  // namespace tensorflow
