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
#define private public
#include "tensorflow/core/platform/s3cloud/s3_file_system.h"
#include <fstream>
#include "tensorflow/core/lib/core/status_test_util.h"
#include "tensorflow/core/platform/s3cloud/http_request_fake.h"
#include "tensorflow/core/platform/test.h"

namespace tensorflow {
  namespace S3 {
    namespace {
      TEST(S3,test){
        int a=0;
        int b=0;
        if((a=1) && (b=0)){
          printf("1 && 0 \n");
        }
        if((a=1) && (b=1)){
          printf("1 && 1 \n");
        }
        if((a=1) || (b=0)){
          printf("1 || 0 \n");
        }
        if((a=1) || (b=1)){
          printf("1 || 1 \n");
        }
        int c = 0;
        c=(b=1);
        printf("c is :%d\n",c);

      }
      TEST(S3,RandomAccessFile){
        S3FileSystem fs;
        
        std::unique_ptr<WritableFile> wfile;
        TF_EXPECT_OK(fs.NewWritableFile("s3://BUCKET-mygod/test-not-exists", &wfile));
        TF_EXPECT_OK(wfile->Append("hello,"));
        TF_EXPECT_OK(wfile->Append("world"));
        TF_EXPECT_OK(wfile->Flush());
        TF_EXPECT_OK(wfile->Sync());
        TF_EXPECT_OK(wfile->Close());  
        
        
        std::unique_ptr<RandomAccessFile> rfile;
        TF_EXPECT_OK
        (fs.NewRandomAccessFile("s3://BUCKET-mygod/test-not-exists", &rfile));
        char scratch[6];
        StringPiece result;
        TF_EXPECT_OK(rfile->Read(0, sizeof(scratch), &result, scratch));
        EXPECT_EQ("hello,", result);
        TF_EXPECT_OK(fs.DeleteFile("s3://BUCKET-mygod/test-not-exists"));
        
      }
      TEST(S3,MultiPartUpload){
        S3FileSystem fs;
        
        std::unique_ptr<WritableFile> wfile;
        TF_EXPECT_OK(fs.NewWritableFile("s3://BUCKET-mygod/test-not-exists", 
                                        &wfile));
        for(int i=0;i<20*1024*1024;i++){
          TF_EXPECT_OK(wfile->Append("1"));
        }
        TF_EXPECT_OK(wfile->Flush());
        TF_EXPECT_OK(wfile->Sync());
        TF_EXPECT_OK(wfile->Close());  
        FileStatistics stat;
        memset(&stat,0,sizeof(stat));
        TF_EXPECT_OK(fs.Stat("s3://BUCKET-mygod/test-not-exists",&stat));
        EXPECT_EQ(stat.length,20*1024*1024);
        EXPECT_EQ(stat.is_directory,false);
        printf("mtime_nsec:%lld\n",stat.mtime_nsec);
        TF_EXPECT_OK(fs.DeleteFile("s3://BUCKET-mygod/test-not-exists"));        
      }      
      
      TEST(S3,AppendableFile){
        S3FileSystem fs;
        
        std::unique_ptr<WritableFile> wfile;
        TF_EXPECT_OK(fs.NewWritableFile("s3://BUCKET-mygod/test-not-exists", &wfile));
        TF_EXPECT_OK(wfile->Append("hello,"));
        TF_EXPECT_OK(wfile->Append("world"));
        TF_EXPECT_OK(wfile->Flush());
        TF_EXPECT_OK(wfile->Sync());
        TF_EXPECT_OK(wfile->Close());  
        
        
        std::unique_ptr<WritableFile> appendFile ;
        TF_EXPECT_OK
        (fs.NewAppendableFile("s3://BUCKET-mygod/test-not-exists", 
                              &appendFile));
        TF_EXPECT_OK(appendFile->Append("wugang"));
        TF_EXPECT_OK(appendFile->Close());  

        std::unique_ptr<RandomAccessFile> rfile;
        TF_EXPECT_OK
        (fs.NewRandomAccessFile("s3://BUCKET-mygod/test-not-exists", &rfile));
        char scratch[17];
        StringPiece result;
        TF_EXPECT_OK(rfile->Read(0, sizeof(scratch), &result, scratch));
        EXPECT_EQ("hello,worldwugang", result);
        TF_EXPECT_OK(fs.DeleteFile("s3://BUCKET-mygod/test-not-exists"));
        
      }
      
      TEST(S3,Stat){
        S3FileSystem fs;
        std::unique_ptr<WritableFile> wfile;
        TF_EXPECT_OK(fs.NewWritableFile("s3://BUCKET-mygod/test-not-exists", &wfile));
        TF_EXPECT_OK(wfile->Append("hello,"));
        TF_EXPECT_OK(wfile->Append("world"));
        TF_EXPECT_OK(wfile->Flush());
        TF_EXPECT_OK(wfile->Sync());
        TF_EXPECT_OK(wfile->Close());  
        FileStatistics stat;
        memset(&stat,0,sizeof(stat));
        TF_EXPECT_OK(fs.Stat("s3://BUCKET-mygod/test-not-exists",&stat));
        EXPECT_EQ(stat.length,11);
        EXPECT_EQ(stat.is_directory,false);
        printf("mtime_nsec:%lld\n",stat.mtime_nsec);
        
        TF_EXPECT_OK(fs.DeleteFile("s3://BUCKET-mygod/test-not-exists"));
      }
      
      TEST(S3,CreateDir){
        S3FileSystem fs;
        TF_EXPECT_OK(fs.CreateDir("s3://BUCKET-mygod/dir-not-exists/"));
        TF_EXPECT_OK(fs.FileExists("s3://BUCKET-mygod/dir-not-exists/"));
        TF_EXPECT_OK(fs.IsDirectory("s3://BUCKET-mygod/dir-not-exists/"));
        FileStatistics stat;
        memset(&stat,0,sizeof(stat));
        TF_EXPECT_OK(fs.Stat("s3://BUCKET-mygod/dir-not-exists",&stat));
        EXPECT_EQ(stat.length,0);
        EXPECT_EQ(stat.is_directory,true);
        TF_EXPECT_OK(fs.DeleteDir("s3://BUCKET-mygod/dir-not-exists"));
      }

      TEST(S3,GetFileSize){
        S3FileSystem fs;
        std::unique_ptr<WritableFile> wfile;
        TF_EXPECT_OK(fs.NewWritableFile("s3://BUCKET-mygod/test-not-exists", &wfile));
        TF_EXPECT_OK(wfile->Append("hello,"));
        TF_EXPECT_OK(wfile->Append("world"));
        TF_EXPECT_OK(wfile->Flush());
        TF_EXPECT_OK(wfile->Sync());
        TF_EXPECT_OK(wfile->Close());  
        uint64 fsize = 0;
        TF_EXPECT_OK(fs.GetFileSize("s3://BUCKET-mygod/test-not-exists",
                                    &fsize));
        EXPECT_EQ(fsize,11);
        TF_EXPECT_OK(fs.RenameFile("s3://BUCKET-mygod/test-not-exists",
                                    "s3://BUCKET-mygod/rename-test-not-exists"));
        TF_EXPECT_OK(fs.DeleteFile("s3://BUCKET-mygod/rename-test-not-exists"));
      }
      
      TEST(S3,DeleteRecursively){
        S3FileSystem fs;
        TF_EXPECT_OK(fs.CreateDir("s3://BUCKET-mygod/dir-not-exists/dir-not-exists"));
        //TF_EXPECT_OK(fs.DeleteDir("s3://BUCKET-mygod/dir-not-exists/"));   
        int64 undeleted_files = 0;
        int64 undeleted_dirs = 0;
        TF_EXPECT_OK(fs.DeleteRecursively("s3://BUCKET-mygod/dir-not-exists/",
                                          &undeleted_files,&undeleted_dirs));   
        EXPECT_EQ(undeleted_files,0);
        EXPECT_EQ(undeleted_dirs,0);
      }
      

      /*
        TEST(S3FileSystemTest, NewRandomAccessFile_NoReadAhead) {
        S3FileSystem fs;
        std::unique_ptr<RandomAccessFile> file;
        TF_EXPECT_OK
        (fs.NewRandomAccessFile("s3://BUCKET-mygod/testfile", &file));

        char scratch[6];
        StringPiece result;

        // Read the first chunk.
        TF_EXPECT_OK(file->Read(0, sizeof(scratch), &result, scratch));
        //EXPECT_EQ("\x0\x0\x0\x0\x0\x0", result);
        }

        TEST(S3FileSystemTest, S3WritableFile) {
        S3FileSystem fs;
        std::unique_ptr<WritableFile> file;
        TF_EXPECT_OK(fs.NewWritableFile("s3://BUCKET-mygod/test-not-exists", &file));
        TF_EXPECT_OK(file->Append("hello,"));
        TF_EXPECT_OK(file->Append("world"));
        //TF_EXPECT_OK(file->Flush());

        //TF_EXPECT_OK(file->Flush());
        //TF_EXPECT_OK(file->Sync());
        //TF_EXPECT_OK(file->Close());  
        }



        TEST(S3FileSystemTest, StatForObject) {
        S3FileSystem fs;
        FileStatistics stat;
        memset(&stat,0,sizeof(FileStatistics));
        TF_EXPECT_OK(fs.StatForObject("BUCKET-mygod","testfile1",&stat));
    
        //TF_EXPECT_OK(file->Flush());

        //TF_EXPECT_OK(file->Flush());
        //TF_EXPECT_OK(file->Sync());
        //TF_EXPECT_OK(file->Close());  
        }

        TEST(S3FileSystemTest, bucketexists) {
        S3FileSystem fs;
        FileStatistics stat;
        memset(&stat,0,sizeof(FileStatistics));
        bool exists = false;
        TF_EXPECT_OK(fs.BucketExists("BUCKET-mygod",&exists));
        EXPECT_EQ(exists,true);
        //TF_EXPECT_OK(file->Flush());

        //TF_EXPECT_OK(file->Flush());
        //TF_EXPECT_OK(file->Sync());
        //TF_EXPECT_OK(file->Close());  
        }

        TEST(S3FileSystemTest, GetChildrenBounded) {
        S3FileSystem fs;
        std::vector<string> result;
        TF_EXPECT_OK(fs.GetChildrenBounded("s3://BUCKET-mygod/",1000,
        &result,true,true));
        for(size_t i=0;i<result.size();i++){
        printf("result:%s\n",result.at(i).c_str());
        }
        result.clear();
        TF_EXPECT_OK(fs.GetChildrenBounded("s3://BUCKET-mygod/",1000,
        &result,false,false));
        for(size_t i=0;i<result.size();i++){
        printf("result:%s\n",result.at(i).c_str());
        }
        }

        TEST(S3FileSystemTest, DeleteFile) {
        S3FileSystem fs;
        TF_EXPECT_OK(fs.DeleteFile("s3://BUCKET-mygod/nihao/test/t"));
        }

      TEST(S3FileSystemTest, DeleteFile) {
        S3FileSystem fs;
        TF_EXPECT_OK(fs.RenameObject("s3://BUCKET-mygod/testfile",
                                     "s3://BUCKET-mygod2/testfile"));
      }      */

    }  // namespace
  }
}  // namespace tensorflow
