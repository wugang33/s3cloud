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

#include "tensorflow/core/platform/s3cloud/s3_file_system.h"
#include <stdio.h>
#include <unistd.h>
#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <vector>
#include "include/json/json.h"
#include "tensorflow/core/lib/core/errors.h"
#include "tensorflow/core/lib/gtl/map_util.h"
#include "tensorflow/core/lib/gtl/stl_util.h"
#include "tensorflow/core/lib/io/path.h"
#include "tensorflow/core/lib/strings/numbers.h"
#include "tensorflow/core/lib/strings/str_util.h"
#include "tensorflow/core/platform/s3cloud/s3_auth_provider.h"
#include "tensorflow/core/platform/s3cloud/time_util.h"
#include "tensorflow/core/platform/env.h"
#include "tensorflow/core/platform/mutex.h"
#include "tensorflow/core/platform/protobuf.h"
#include "tensorflow/core/platform/thread_annotations.h"
#include "tinyxml2.h"
namespace tensorflow {
namespace S3 {
namespace {


constexpr size_t kReadAppendableFileBufferSize = 1024 * 1024;  // In bytes.
constexpr int kGetChildrenDefaultPageSize = 1000;
// Initial delay before retrying a GCS upload.
// Subsequent delays can be larger due to exponential back-off.
constexpr uint64 kUploadRetryDelayMicros = 1000000L;
// The HTTP response code "308 Resume Incomplete".
constexpr uint64 HTTP_CODE_RESUME_INCOMPLETE = 308;
  //MultiPart upload max size pre part
  constexpr uint64 kMultiPartUploadMaxSize = 15*1024*1024;
  constexpr uint64 kMultiPartUploadRetryCount = 3;
  /*
constexpr char kStorageHost[] = "host142";
constexpr char kS3Key[] = "XN6QBMRYUE5H49KNVT1D";
constexpr char kS3Secret[] = "bo7ISjXTyrKElDkzLAsBCvXDvN22xh6e9VUA6qFS";
  */
// The file statistics returned by Stat() for directories.
const FileStatistics DIRECTORY_STAT(0, 0, true);

Status GetTmpFilename(string* filename) {
  if (!filename) {
    return errors::Internal("'filename' cannot be nullptr.");
  }
  char buffer[] = "/tmp/gcs_filesystem_XXXXXX";
  int fd = mkstemp(buffer);
  if (fd < 0) {
    return errors::Internal("Failed to create a temporary file.");
  }
  close(fd);
  *filename = buffer;
  return Status::OK();
}

/// \brief Splits a GCS path to a bucket and an object.
///
/// For example, "gs://bucket-name/path/to/file.txt" gets split into
/// "bucket-name" and "path/to/file.txt".
/// If fname only contains the bucket and empty_object_ok = true, the returned
/// object is empty.
Status ParseS3Path(StringPiece fname, bool empty_object_ok, string* bucket,
                    string* object) {
  if (!bucket || !object) {
    return errors::Internal("bucket and object cannot be null.");
  }
  StringPiece scheme, bucketp, objectp;
  io::ParseURI(fname, &scheme, &bucketp, &objectp);
  if (scheme != "s3") {
    return errors::InvalidArgument("GCS path doesn't start with 's3://': ",
                                   fname);
  }
  *bucket = bucketp.ToString();
  if (bucket->empty() || *bucket == ".") {
    return errors::InvalidArgument("GCS path doesn't contain a bucket name: ",
                                   fname);
  }
  objectp.Consume("/");
  *object = objectp.ToString();
  if (!empty_object_ok && object->empty()) {
    return errors::InvalidArgument("GCS path doesn't contain an object name: ",
                                   fname);
  }
  return Status::OK();
}

/// Appends a trailing slash if the name doesn't already have one.
string MaybeAppendSlash(const string& name) {
  if (name.empty()) {
    return "/";
  }
  if (name.back() != '/') {
    return strings::StrCat(name, "/");
  }
  return name;
}

// io::JoinPath() doesn't work in cases when we want an empty subpath
// to result in an appended slash in order for directory markers
// to be processed correctly: "gs://a/b" + "" should give "gs://a/b/".
string JoinS3Path(const string& path, const string& subpath) {
  return strings::StrCat(MaybeAppendSlash(path), subpath);
}

/// \brief Returns the given paths appending all their subfolders.
///
/// For every path X in the list, every subfolder in X is added to the
/// resulting list.
/// For example:
///  - for 'a/b/c/d' it will append 'a', 'a/b' and 'a/b/c'
///  - for 'a/b/c/' it will append 'a', 'a/b' and 'a/b/c'
std::set<string> AddAllSubpaths(const std::vector<string>& paths) {
  std::set<string> result;
  result.insert(paths.begin(), paths.end());
  for (const string& path : paths) {
    StringPiece subpath = io::Dirname(path);
    while (!subpath.empty()) {
      result.emplace(subpath.ToString());
      subpath = io::Dirname(subpath);
    }
  }
  return result;
}

Status ParseJson(StringPiece json, Json::Value* result) {
  Json::Reader reader;
  if (!reader.parse(json.ToString(), *result)) {
    return errors::Internal("Couldn't parse JSON response from GCS.");
  }
  return Status::OK();
}

/// Reads a JSON value with the given name from a parent JSON value.
Status GetValue(const Json::Value& parent, const string& name,
                Json::Value* result) {
  *result = parent.get(name, Json::Value::null);
  if (*result == Json::Value::null) {
    return errors::Internal("The field '", name,
                            "' was expected in the JSON response.");
  }
  return Status::OK();
}

/// Reads a string JSON value with the given name from a parent JSON value.
Status GetStringValue(const Json::Value& parent, const string& name,
                      string* result) {
  Json::Value result_value;
  TF_RETURN_IF_ERROR(GetValue(parent, name, &result_value));
  if (!result_value.isString()) {
    return errors::Internal(
        "The field '", name,
        "' in the JSON response was expected to be a string.");
  }
  *result = result_value.asString();
  return Status::OK();
}

  /// Reads a long JSON value with the given name from a parent JSON value.
  Status GetInt64Value(const string& strVal,int64* result) {
    if(strings::safe_strto64(strVal.c_str(), result)) {
      return Status::OK();
    }
    return errors::Internal
      ("The strVal '",strVal,"' expected to be a number.");
  }

/// Reads a boolean JSON value with the given name from a parent JSON value.
Status GetBoolValue(const Json::Value& parent, const string& name,
                    bool* result) {
  Json::Value result_value;
  TF_RETURN_IF_ERROR(GetValue(parent, name, &result_value));
  if (!result_value.isBool()) {
    return errors::Internal(
        "The field '", name,
        "' in the JSON response was expected to be a boolean.");
  }
  *result = result_value.asBool();
  return Status::OK();
}

/// A GCS-based implementation of a random access file with a read-ahead buffer.
class S3RandomAccessFile : public RandomAccessFile {
 public:
  S3RandomAccessFile(const string& bucket, const string& object,
                      AuthProvider* auth_provider,
                      HttpRequest::Factory* http_request_factory,
                      size_t read_ahead_bytes)
      : bucket_(bucket),
        object_(object),
        auth_provider_(auth_provider),
        http_request_factory_(http_request_factory),
        read_ahead_bytes_(read_ahead_bytes) {}

  /// The implementation of reads with a read-ahead buffer. Thread-safe.
  Status Read(uint64 offset, size_t n, StringPiece* result,
              char* scratch) const override {
    mutex_lock lock(mu_);
    const bool range_start_included = offset >= buffer_start_offset_;
    const bool range_end_included =
        offset + n <= buffer_start_offset_ + buffer_.size();
    if (range_start_included && range_end_included) {
      // The requested range can be filled from the buffer.
      const size_t offset_in_buffer =
          std::min<uint64>(offset - buffer_start_offset_, buffer_.size());
      const auto copy_size = std::min(n, buffer_.size() - offset_in_buffer);
      std::copy(buffer_.begin() + offset_in_buffer,
                buffer_.begin() + offset_in_buffer + copy_size, scratch);
      *result = StringPiece(scratch, copy_size);
    } else {
      // Update the buffer content based on the new requested range.
      const size_t desired_buffer_size = n + read_ahead_bytes_;
      if (n > buffer_.capacity() ||
          desired_buffer_size > 2 * buffer_.capacity()) {
        // Re-allocate only if buffer capacity increased significantly.
        buffer_.reserve(desired_buffer_size);
      }

      buffer_start_offset_ = offset;
      TF_RETURN_IF_ERROR(LoadBufferFromGCS());

      // Set the results.
      std::memcpy(scratch, buffer_.data(), std::min(buffer_.size(), n));
      *result = StringPiece(scratch, std::min(buffer_.size(), n));
    }

    if (result->size() < n) {
      // This is not an error per se. The RandomAccessFile interface expects
      // that Read returns OutOfRange if fewer bytes were read than requested.
      return errors::OutOfRange("EOF reached, ", result->size(),
                                " bytes were read out of ", n,
                                " bytes requested.");
    }
    return Status::OK();
  }

 private:
  /*
   * Connected to host142 (192.168.150.142) port 80 (#0)
   > GET /BUCKET-mygod/testfile HTTP/1.1
   > User-Agent: curl/7.29.0
   > Accept:
   > Host: host142
   > Date: Thu, 09 Feb 2017 18:50:17 +0000
   > Content-Type: application/x-compressed-tar
   > Authorization: AWS XN6QBMRYUE5H49KNVT1D:1WrUNZ59pPn2Stz7FIadv3lyYvM=
   > 
   < HTTP/1.1 200 OK
   < Content-Length: 55882240
   < Accept-Ranges: bytes
   < Last-Modified: Thu, 09 Feb 2017 09:44:08 GMT
   < ETag: "75b37f013893e8c2750454dd0f379191"
   < x-amz-request-id: tx000000000000000000156-00589cb9e9-1045-default
   < Content-Type: application/x-compressed-tar
   < Date: Thu, 09 Feb 2017 18:50:17 GMT
   < 
   */
  /// A helper function to actually read the data from GCS. This function loads
  /// buffer_ from GCS based on its current capacity.
  Status LoadBufferFromGCS() const EXCLUSIVE_LOCKS_REQUIRED(mu_) {
    string kStorageHost,kS3Key,kS3Secret;
    TF_RETURN_IF_ERROR(AuthProvider::GetHost(auth_provider_, &kStorageHost));
    TF_RETURN_IF_ERROR(AuthProvider::GetKey(auth_provider_, &kS3Key));
    TF_RETURN_IF_ERROR(AuthProvider::GetSecret(auth_provider_, &kS3Secret));
    
    string signedValue;
    std::unique_ptr<HttpRequest> request(http_request_factory_->Create());
    TF_RETURN_IF_ERROR(request->Init());
    TF_RETURN_IF_ERROR
      (request->SetUri(strings::StrCat
                       ("http://", kStorageHost,"/", bucket_,"/",
                        object_)));

    TF_RETURN_IF_ERROR(request->SetRange(
        buffer_start_offset_, buffer_start_offset_ + buffer_.capacity() - 1));
    TF_RETURN_IF_ERROR(request->SetResultBuffer(&buffer_));
    time_t now = time(0);
    string nowStr;
    string contentType="application/x-compressed-tar";
    string resource = strings::StrCat("/",bucket_,"/",object_);
    TF_RETURN_IF_ERROR(GetRfc850Time(now,nowStr));
    TF_RETURN_IF_ERROR(request->AddHeader("Date",nowStr));
    TF_RETURN_IF_ERROR(request->AddHeader("Content-Type",contentType));
    string toSigned = strings::StrCat("GET\n\n",contentType,"\n",nowStr
                                      ,"\n",resource);
    //printf("resource:%s\n",resource.c_str());
    //printf("strtoSign:%s\n",toSigned.c_str());
    TF_RETURN_IF_ERROR(ComputeS3Signed(kS3Secret,toSigned,signedValue));
    TF_RETURN_IF_ERROR(request->AddS3AuthBearerHeader(kS3Key,signedValue));
    //printf("signed:%s\n",signedValue.c_str());
    TF_RETURN_WITH_CONTEXT_IF_ERROR(request->Send(), " when reading s3://",
                                    bucket_, "/", object_);
    return Status::OK();
  }

  string bucket_;
  string object_;
  AuthProvider* auth_provider_;
  HttpRequest::Factory* http_request_factory_;
  const size_t read_ahead_bytes_;

  // The buffer-related members need to be mutable, because they are modified
  // by the const Read() method.
  mutable mutex mu_;
  mutable std::vector<char> buffer_ GUARDED_BY(mu_);
  // The original file offset of the first byte in the buffer.
  mutable size_t buffer_start_offset_ GUARDED_BY(mu_) = 0;
};

/// \brief GCS-based implementation of a writeable file.
///
/// Since GCS objects are immutable, this implementation writes to a local
/// tmp file and copies it to GCS on flush/close.
class S3WritableFile : public WritableFile {
 public:
  S3WritableFile(const string& bucket, const string& object,
                  AuthProvider* auth_provider,
                  HttpRequest::Factory* http_request_factory,
                  int32 max_upload_attempts)
      : bucket_(bucket),
        object_(object),
        auth_provider_(auth_provider),
        http_request_factory_(http_request_factory),
        sync_needed_(true),
        max_upload_attempts_(max_upload_attempts) {
    if (GetTmpFilename(&tmp_content_filename_).ok()) {
      outfile_.open(tmp_content_filename_,
                    std::ofstream::binary | std::ofstream::app);
    }
  }

  /// \brief Constructs the writable file in append mode.
  ///
  /// tmp_content_filename should contain a path of an existing temporary file
  /// with the content to be appended. The class takes onwnership of the
  /// specified tmp file and deletes it on close.
  S3WritableFile(const string& bucket, const string& object,
                  AuthProvider* auth_provider,
                  const string& tmp_content_filename,
                  HttpRequest::Factory* http_request_factory,
                  int32 max_upload_attempts)
      : bucket_(bucket),
        object_(object),
        auth_provider_(auth_provider),
        http_request_factory_(http_request_factory),
        sync_needed_(true),
        max_upload_attempts_(max_upload_attempts) {
    tmp_content_filename_ = tmp_content_filename;
    outfile_.open(tmp_content_filename_,
                  std::ofstream::binary | std::ofstream::app);
  }

  ~S3WritableFile() { Close(); }

  Status Append(const StringPiece& data) override {
    TF_RETURN_IF_ERROR(CheckWritable());
    sync_needed_ = true;
    outfile_ << data;
    if (!outfile_.good()) {
      return errors::Internal(
          "Could not append to the internal temporary file.");
    }
    return Status::OK();
  }

  Status Close() override {
    if (outfile_.is_open()) {
      TF_RETURN_IF_ERROR(Sync());
      outfile_.close();
      std::remove(tmp_content_filename_.c_str());
    }
    return Status::OK();
  }

  Status Flush() override { return Sync(); }

  Status Sync() override {
    TF_RETURN_IF_ERROR(CheckWritable());
    if (!sync_needed_) {
      return Status::OK();
    }
    Status status = SyncImpl();
    if (status.ok()) {
      sync_needed_ = false;
    }
    return status;
  }

 private:
  /// Copies the current version of the file to S3.
  ///
  /// This SyncImpl() uploads the object to S3.
  /// In case of a failure, it resumes failed uploads as recommended by the S3
  /// resumable API documentation. When the whole upload needs to be
  /// restarted, Sync() returns UNAVAILABLE and relies on RetryingFileSystem.
  Status SyncImpl() {
    outfile_.flush();
    if (!outfile_.good()) {
      return errors::Internal(
          "Could not write to the internal temporary file.");
    }
    //constexpr uint64 kMultiPartUploadMaxSize = 10*1024*1024;
    uint64 file_size = 0;
    TF_RETURN_IF_ERROR(GetCurrentFileSize(&file_size));
    if(file_size <=kMultiPartUploadMaxSize){
      return PutFile();
    }
    std::vector<std::pair<int,string> > trans;
    uint64 remainedSize = file_size;
    uint64 partNum = 1;
    string uploadId;
    TF_RETURN_IF_ERROR(CreateNewUploadSession(&uploadId));
    while(remainedSize>0){
      Status status;
      uint64 partSize = kMultiPartUploadMaxSize;
      if(remainedSize<partSize){
        partSize  = remainedSize;
      }
      string etag;
      for(uint64 i=0;i<kMultiPartUploadRetryCount;i++){
        status = UploadToSession(uploadId,file_size - remainedSize,
                                 partSize,partNum,etag);
        if(status==Status::OK()){
          break;
        }
      }
      if(status==Status::OK()){
        trans.push_back(std::make_pair(partNum,etag));
        remainedSize -= partSize;
        partNum++;
      }else{
        AbortUploadSession(&uploadId); 
        return status;
      }
    }//end while
    TF_RETURN_IF_ERROR(FinishUploadSession(uploadId,trans));
    return Status::OK();
  }

  Status CheckWritable() const {
    if (!outfile_.is_open()) {
      return errors::FailedPrecondition(
          "The internal temporary file is not writable.");
    }
    return Status::OK();
  }

  Status GetCurrentFileSize(uint64* size) {
    if (size == nullptr) {
      return errors::Internal("'size' cannot be nullptr");
    }
    const auto tellp = outfile_.tellp();
    if (tellp == -1) {
      return errors::Internal(
          "Could not get the size of the internal temporary file.");
    }
    *size = tellp;
    return Status::OK();
  }

  /// Initiates a new resumable upload session.
  /// TODO to optimization with curl_multi_init and s3's multipart internface
  //  Now we implements as a single thread 
  Status CreateNewUploadSession(string* uploadId) {
    string kStorageHost,kS3Key,kS3Secret;
    TF_RETURN_IF_ERROR(AuthProvider::GetHost(auth_provider_, &kStorageHost));
    TF_RETURN_IF_ERROR(AuthProvider::GetKey(auth_provider_, &kS3Key));
    TF_RETURN_IF_ERROR(AuthProvider::GetSecret(auth_provider_, &kS3Secret));
    if (uploadId == nullptr) {
      return errors::Internal("'session_uri' cannot be nullptr.");
    }
    string signedValue;
    
    uint64 file_size;
    TF_RETURN_IF_ERROR(GetCurrentFileSize(&file_size));
    
    std::vector<char> output_buffer;
    std::unique_ptr<HttpRequest> request(http_request_factory_->Create());
    TF_RETURN_IF_ERROR(request->Init());
    TF_RETURN_IF_ERROR
      (request->SetUri(strings::StrCat
                       ("http://", kStorageHost,"/", bucket_,"/",object_,"?uploads")));
    time_t now = time(0);
    string nowStr;
    string contentType="application/octet-stream";
    string resource = strings::StrCat("/",bucket_,"/",object_,"?uploads");
    TF_RETURN_IF_ERROR(GetRfc850Time(now,nowStr));
    TF_RETURN_IF_ERROR(request->AddHeader("Date",nowStr));
    TF_RETURN_IF_ERROR(request->AddHeader("Content-Type",contentType));
    string toSigned = strings::StrCat("POST\n\n",contentType,"\n",nowStr
                                      ,"\n",resource);
    TF_RETURN_IF_ERROR(ComputeS3Signed(kS3Secret,toSigned,signedValue));
    TF_RETURN_IF_ERROR(request->AddS3AuthBearerHeader(kS3Key,signedValue));
    
    TF_RETURN_IF_ERROR(request->SetPostEmptyBody());
    TF_RETURN_IF_ERROR(request->SetResultBuffer(&output_buffer));
    TF_RETURN_WITH_CONTEXT_IF_ERROR(
        request->Send(), " when initiating an upload to ", GetS3Path());
    string output = string(output_buffer.data(),output_buffer.size());
    //printf("new session output:%s\n",output.c_str());
    tinyxml2::XMLDocument doc;
    doc.Parse(output.c_str());
    if(doc.Error()){
      return errors::Internal("xml:",output," Parse failure due to error:",
                              doc.ErrorName());
    }
    tinyxml2::XMLElement* resutlXml = 
      doc.FirstChildElement( "InitiateMultipartUploadResult" );
    if(NULL == resutlXml){
      return errors::Internal("Unexpected response from s3 when writing to ",
                              GetS3Path(),
                              " response xml not reconginize:",output);
    }
    tinyxml2::XMLElement* bucketXml  = resutlXml->FirstChildElement("Bucket");
    tinyxml2::XMLElement* keyXml  = resutlXml->FirstChildElement("Key");
    tinyxml2::XMLElement* uploadIdXml  = 
      resutlXml->FirstChildElement("UploadId");
    if(NULL == bucketXml || NULL == keyXml || NULL == uploadIdXml ){
      return errors::Internal("Unexpected response from s3 when writing to ",
                              GetS3Path(),
                              " response xml not reconginize:",output);
    }
    /*printf("upload bucket:%s key:%s uploadid:%s\n",bucketXml->GetText(),
      keyXml->GetText(),uploadIdXml->GetText());*/
    *uploadId = uploadIdXml->GetText();
    return Status::OK();
  }
  
  Status AbortUploadSession(string* uploadId) {
    string kStorageHost,kS3Key,kS3Secret;
    TF_RETURN_IF_ERROR(AuthProvider::GetHost(auth_provider_, &kStorageHost));
    TF_RETURN_IF_ERROR(AuthProvider::GetKey(auth_provider_, &kS3Key));
    TF_RETURN_IF_ERROR(AuthProvider::GetSecret(auth_provider_, &kS3Secret));
    if (uploadId == nullptr) {
      return errors::Internal("'session_uri' cannot be nullptr.");
    }
    string signedValue;
    std::unique_ptr<HttpRequest> request(http_request_factory_->Create());
    TF_RETURN_IF_ERROR(request->Init());
    string resource = 
      strings::StrCat("/",bucket_,"/",object_,"?uploadId=",*uploadId);
    TF_RETURN_IF_ERROR
      (request->SetUri(strings::StrCat
                       ("http://", kStorageHost,resource)));
    time_t now = time(0);
    string nowStr;
    string contentType="application/octet-stream";

    TF_RETURN_IF_ERROR(GetRfc850Time(now,nowStr));
    TF_RETURN_IF_ERROR(request->AddHeader("Date",nowStr));
    TF_RETURN_IF_ERROR(request->AddHeader("Content-Type",contentType));
    string toSigned = strings::StrCat("DELETE\n\n",contentType,"\n",nowStr
                                      ,"\n",resource);
    TF_RETURN_IF_ERROR(ComputeS3Signed(kS3Secret,toSigned,signedValue));
    TF_RETURN_IF_ERROR(request->AddS3AuthBearerHeader(kS3Key,signedValue));
    TF_RETURN_IF_ERROR(request->SetDeleteRequest());
    TF_RETURN_WITH_CONTEXT_IF_ERROR
      (request->Send(), " when abort an upload:",*uploadId," to ", GetS3Path());
    return Status::OK();
  }

  Status FinishUploadSession(const string& uploadId,
                             const std::vector<std::pair<int,string> >
                             &transformed) {
    string kStorageHost,kS3Key,kS3Secret;
    TF_RETURN_IF_ERROR(AuthProvider::GetHost(auth_provider_, &kStorageHost));
    TF_RETURN_IF_ERROR(AuthProvider::GetKey(auth_provider_, &kS3Key));
    TF_RETURN_IF_ERROR(AuthProvider::GetSecret(auth_provider_, &kS3Secret));
    //printf("FinishUploadSession\n");
    string signedValue;
    std::vector<char> output_buffer;
    std::unique_ptr<HttpRequest> request(http_request_factory_->Create());
    TF_RETURN_IF_ERROR(request->Init());
    TF_RETURN_IF_ERROR
      (request->SetUri(strings::StrCat
                       ("http://", kStorageHost,"/", bucket_,"/",object_,"?uploadId=",uploadId)));
    time_t now = time(0);
    string nowStr;
    string contentType="application/octet-stream";
    string resource = strings::StrCat("/",bucket_,"/",object_,
                                      "?uploadId=",uploadId);
    TF_RETURN_IF_ERROR(GetRfc850Time(now,nowStr));
    TF_RETURN_IF_ERROR(request->AddHeader("Date",nowStr));
    TF_RETURN_IF_ERROR(request->AddHeader("Content-Type",contentType));
    string toSigned = strings::StrCat("POST\n\n",contentType,"\n",nowStr
                                      ,"\n",resource);
    TF_RETURN_IF_ERROR(ComputeS3Signed(kS3Secret,toSigned,signedValue));
    TF_RETURN_IF_ERROR(request->AddS3AuthBearerHeader(kS3Key,signedValue));
    
    tinyxml2::XMLDocument doc;
    tinyxml2::XMLNode* element = 
      doc.InsertEndChild(doc.NewElement("CompleteMultipartUpload"));
    tinyxml2::XMLNode* child = NULL;
    for(size_t i=0;i<transformed.size();i++){
      tinyxml2::XMLNode* part = NULL;
      if(child == NULL){
        part = element->InsertFirstChild( doc.NewElement( "Part" ) );
      }else{
        part = element->InsertAfterChild(child, doc.NewElement( "Part" ) );
      }
      child = part;
      
      int partnum = transformed.at(i).first;
      string etag =  transformed.at(i).second; 
      tinyxml2::XMLElement*partXML = doc.NewElement("PartNumber");
      partXML->SetText(partnum);
      tinyxml2::XMLElement*etagXML = doc.NewElement("ETag");
      etagXML->InsertFirstChild(doc.NewText(etag.c_str()));
      
      part->InsertFirstChild(partXML);
      part->InsertAfterChild(partXML,etagXML);
    }
    tinyxml2::XMLPrinter streamer;
    doc.Print( &streamer );
    /*
    printf( "xml value is %s\n", streamer.CStr() );
    FILE*testdebug = fopen("/tmp/debug.xml","w");
    //size_t fwrite ( const void * ptr, size_t size, size_t count, FILE * stream );
    size_t writeNum = fwrite(streamer.CStr(),streamer.CStrSize(),1,testdebug);
    assert(writeNum == streamer.CStrSize());
    fclose(testdebug);
    */
    TF_RETURN_IF_ERROR(request->SetPostFromBuffer(streamer.CStr(),
                                                  streamer.CStrSize()-1));
    TF_RETURN_IF_ERROR(request->SetResultBuffer(&output_buffer));
    TF_RETURN_WITH_CONTEXT_IF_ERROR(
        request->Send(), " when initiating an upload to ", GetS3Path());
    //string output = string(output_buffer.data(),output_buffer.size());
    //printf("finish output:%s\n",output.c_str());
    return Status::OK();
    
  }
  
  Status UploadToSession(const string& uploadId, 
                         uint64 start_offset,uint64 size,
                         int partNum,string&etag) {
    string kStorageHost,kS3Key,kS3Secret;
    TF_RETURN_IF_ERROR(AuthProvider::GetHost(auth_provider_, &kStorageHost));
    TF_RETURN_IF_ERROR(AuthProvider::GetKey(auth_provider_, &kS3Key));
    TF_RETURN_IF_ERROR(AuthProvider::GetSecret(auth_provider_, &kS3Secret));
    string signedValue;
    uint64 file_size;    
    std::vector<char> output_buffer;
    TF_RETURN_IF_ERROR(GetCurrentFileSize(&file_size));

    std::unique_ptr<HttpRequest> request(http_request_factory_->Create());
    TF_RETURN_IF_ERROR(request->Init());
    //resource="/${bucket}/${objname}?partNumber=1&uploadId=${uploadid}" 
    string resource = 
      strings::StrCat("/", bucket_,"/",object_,
                      "?partNumber=",partNum,
                      "&uploadId=",uploadId);
    TF_RETURN_IF_ERROR
      (request->SetUri(strings::StrCat
                       ("http://", kStorageHost,resource)));
    time_t now = time(0);
    string nowStr;
    string contentType="application/octet-stream";
    TF_RETURN_IF_ERROR(GetRfc850Time(now,nowStr));
    TF_RETURN_IF_ERROR(request->AddHeader("Date",nowStr));
    TF_RETURN_IF_ERROR(request->AddHeader("Content-Type",contentType));
    string toSigned = strings::StrCat("PUT\n\n",contentType,"\n",nowStr
                                      ,"\n",resource);
    TF_RETURN_IF_ERROR(ComputeS3Signed(kS3Secret,toSigned,signedValue));
    TF_RETURN_IF_ERROR(request->AddS3AuthBearerHeader(kS3Key,signedValue));

    TF_RETURN_IF_ERROR
      (request->SetPutFromFile(tmp_content_filename_, start_offset,size));
    TF_RETURN_WITH_CONTEXT_IF_ERROR(request->Send(), " when uploading ",
                                    GetS3Path());
    etag  = request->GetResponseHeader("ETag");
    if (etag.empty()) {
      return errors::Internal("Unexpected response from GCS when writing to ",
                              GetS3Path(),
                              ": 'Location' header not returned.");
    }
    str_util::StripTrailingWhitespace(&etag);
    StringPiece pic(etag);
    pic.remove_prefix(1);
    pic.remove_suffix(1);
    etag  = pic.ToString();
    return Status::OK();
  }
  Status PutFile() {
    string kStorageHost,kS3Key,kS3Secret;
    TF_RETURN_IF_ERROR(AuthProvider::GetHost(auth_provider_, &kStorageHost));
    TF_RETURN_IF_ERROR(AuthProvider::GetKey(auth_provider_, &kS3Key));
    TF_RETURN_IF_ERROR(AuthProvider::GetSecret(auth_provider_, &kS3Secret));
    string signedValue;
    uint64 file_size;    
    std::vector<char> output_buffer;
    TF_RETURN_IF_ERROR(GetCurrentFileSize(&file_size));

    std::unique_ptr<HttpRequest> request(http_request_factory_->Create());
    TF_RETURN_IF_ERROR(request->Init());
    
    string resource = 
      strings::StrCat("/", bucket_,"/",object_);
    TF_RETURN_IF_ERROR
      (request->SetUri(strings::StrCat
                       ("http://", kStorageHost,resource)));
    time_t now = time(0);
    string nowStr;
    string contentType="application/octet-stream";
    TF_RETURN_IF_ERROR(GetRfc850Time(now,nowStr));
    TF_RETURN_IF_ERROR(request->AddHeader("Date",nowStr));
    TF_RETURN_IF_ERROR(request->AddHeader("Content-Type",contentType));
    string toSigned = strings::StrCat("PUT\n\n",contentType,"\n",nowStr
                                      ,"\n",resource);
    TF_RETURN_IF_ERROR(ComputeS3Signed(kS3Secret,toSigned,signedValue));
    TF_RETURN_IF_ERROR(request->AddS3AuthBearerHeader(kS3Key,signedValue));

    TF_RETURN_IF_ERROR
      (request->SetPutFromFile(tmp_content_filename_, 0));
    TF_RETURN_WITH_CONTEXT_IF_ERROR(request->Send(), " when uploading ",
                                    GetS3Path());
    return Status::OK();
  }

  string GetS3Path() const {
    return strings::StrCat("s3://", bucket_, "/", object_);
  }

  string bucket_;
  string object_;
  AuthProvider* auth_provider_;
  string tmp_content_filename_;
  std::ofstream outfile_;
  HttpRequest::Factory* http_request_factory_;
  bool sync_needed_;  // whether there is buffered data that needs to be synced
  int32 max_upload_attempts_;
};

class S3ReadOnlyMemoryRegion : public ReadOnlyMemoryRegion {
 public:
  S3ReadOnlyMemoryRegion(std::unique_ptr<char[]> data, uint64 length)
      : data_(std::move(data)), length_(length) {}
  const void* data() override { return reinterpret_cast<void*>(data_.get()); }
  uint64 length() override { return length_; }

 private:
  std::unique_ptr<char[]> data_;
  uint64 length_;
};
}  // namespace

S3FileSystem::S3FileSystem()
    : auth_provider_(new S3AuthProvider()),
      http_request_factory_(new HttpRequest::Factory()) {}

S3FileSystem::S3FileSystem(
    std::unique_ptr<AuthProvider> auth_provider,
    std::unique_ptr<HttpRequest::Factory> http_request_factory,
    size_t read_ahead_bytes, int32 max_upload_attempts)
    : auth_provider_(std::move(auth_provider)),
      http_request_factory_(std::move(http_request_factory)),
      read_ahead_bytes_(read_ahead_bytes),
      max_upload_attempts_(max_upload_attempts) {}

Status S3FileSystem::NewRandomAccessFile(
    const string& fname, std::unique_ptr<RandomAccessFile>* result) {
  string bucket, object;
  TF_RETURN_IF_ERROR(ParseS3Path(fname, false, &bucket, &object));
  result->reset(new S3RandomAccessFile(bucket, object, auth_provider_.get(),
                                        http_request_factory_.get(),
                                        read_ahead_bytes_));
  return Status::OK();
}

Status S3FileSystem::NewWritableFile(const string& fname,
                                      std::unique_ptr<WritableFile>* result) {
  string bucket, object;
  TF_RETURN_IF_ERROR(ParseS3Path(fname, false, &bucket, &object));
  result->reset(new S3WritableFile(bucket, object, auth_provider_.get(),
                                    http_request_factory_.get(),
                                    max_upload_attempts_));
  return Status::OK();
}

// Reads the file from GCS in chunks and stores it in a tmp file,
// which is then passed to S3WritableFile.
Status S3FileSystem::NewAppendableFile(const string& fname,
                                        std::unique_ptr<WritableFile>* result) {
  std::unique_ptr<RandomAccessFile> reader;
  TF_RETURN_IF_ERROR(NewRandomAccessFile(fname, &reader));
  std::unique_ptr<char[]> buffer(new char[kReadAppendableFileBufferSize]);
  Status status;
  uint64 offset = 0;
  StringPiece read_chunk;

  // Read the file from GCS in chunks and save it to a tmp file.
  string old_content_filename;
  TF_RETURN_IF_ERROR(GetTmpFilename(&old_content_filename));
  std::ofstream old_content(old_content_filename, std::ofstream::binary);
  while (true) {
    status = reader->Read(offset, kReadAppendableFileBufferSize, &read_chunk,
                          buffer.get());
    if (status.ok()) {
      old_content << read_chunk;
      offset += kReadAppendableFileBufferSize;
    } else if (status.code() == error::OUT_OF_RANGE) {
      // Expected, this means we reached EOF.
      old_content << read_chunk;
      break;
    } else {
      return status;
    }
  }
  old_content.close();

  // Create a writable file and pass the old content to it.
  string bucket, object;
  TF_RETURN_IF_ERROR(ParseS3Path(fname, false, &bucket, &object));
  result->reset(new S3WritableFile(
      bucket, object, auth_provider_.get(), old_content_filename,
      http_request_factory_.get(), max_upload_attempts_));
  return Status::OK();
}

Status S3FileSystem::NewReadOnlyMemoryRegionFromFile(
    const string& fname, std::unique_ptr<ReadOnlyMemoryRegion>* result) {
  uint64 size;
  TF_RETURN_IF_ERROR(GetFileSize(fname, &size));
  std::unique_ptr<char[]> data(new char[size]);

  std::unique_ptr<RandomAccessFile> file;
  TF_RETURN_IF_ERROR(NewRandomAccessFile(fname, &file));

  StringPiece piece;
  TF_RETURN_IF_ERROR(file->Read(0, size, &piece, data.get()));

  result->reset(new S3ReadOnlyMemoryRegion(std::move(data), size));
  return Status::OK();
}

Status S3FileSystem::FileExists(const string& fname) {
  string bucket, object;
  TF_RETURN_IF_ERROR(ParseS3Path(fname, true, &bucket, &object));
  if (object.empty()) {
    bool result;
    TF_RETURN_IF_ERROR(BucketExists(bucket, &result));
    if (result) {
      return Status::OK();
    }
  }
  bool result;
  TF_RETURN_IF_ERROR(ObjectExists(bucket, object, &result));
  if (result) {
    return Status::OK();
  }
  TF_RETURN_IF_ERROR(FolderExists(fname, &result));
  if (result) {
    return Status::OK();
  }
  return errors::NotFound("The specified path ", fname, " was not found.");
}

Status S3FileSystem::ObjectExists(const string& bucket, const string& object,
                                   bool* result) {
  if (!result) {
    return errors::Internal("'result' cannot be nullptr.");
  }
  FileStatistics not_used_stat;
  const Status status = StatForObject(bucket, object, &not_used_stat);
  switch (status.code()) {
    case errors::Code::OK:
      *result = true;
      return Status::OK();
    case errors::Code::NOT_FOUND:
      *result = false;
      return Status::OK();
    default:
      return status;
  }
}

Status S3FileSystem::StatForObject(const string& bucket, const string& object,
                                    FileStatistics* stat) {
  string kStorageHost,kS3Key,kS3Secret;
  TF_RETURN_IF_ERROR(AuthProvider::GetHost(auth_provider_.get(), &kStorageHost));
  TF_RETURN_IF_ERROR(AuthProvider::GetKey(auth_provider_.get(), &kS3Key));
  TF_RETURN_IF_ERROR(AuthProvider::GetSecret(auth_provider_.get(), &kS3Secret));
  if (!stat) {
    return errors::Internal("'stat' cannot be nullptr.");
  }
  if (object.empty()) {
    return errors::InvalidArgument("'object' must be a non-empty string.");
  }

  string signedValue;

  std::vector<char> output_buffer;
  std::unique_ptr<HttpRequest> request(http_request_factory_->Create());
  TF_RETURN_IF_ERROR(request->Init());
  string resource = strings::StrCat("/", bucket,"/",
                                    object);
  TF_RETURN_IF_ERROR
    (request->SetUri(strings::StrCat
                     ("http://", kStorageHost,resource)));
    
  time_t now = time(0);
  string nowStr;
  string contentType="application/x-compressed-tar";
  TF_RETURN_IF_ERROR(GetRfc850Time(now,nowStr));
  TF_RETURN_IF_ERROR(request->AddHeader("Date",nowStr));
  TF_RETURN_IF_ERROR(request->AddHeader("Content-Type",contentType));
  string toSigned = strings::StrCat("HEAD\n\n",contentType,"\n",nowStr
                                    ,"\n",resource);
  TF_RETURN_IF_ERROR(ComputeS3Signed(kS3Secret,toSigned,signedValue));
  TF_RETURN_IF_ERROR(request->AddS3AuthBearerHeader(kS3Key,signedValue));
  
  TF_RETURN_IF_ERROR(request->SetHeadRequest());
  TF_RETURN_IF_ERROR(request->SetResultBuffer(&output_buffer));
  TF_RETURN_WITH_CONTEXT_IF_ERROR
    (request->Send(), " when reading metadata of gs://", bucket, "/", object);
  string sizeStr  = request->GetResponseHeader("Content-Length");
  string lastModifiedStr = request->GetResponseHeader("Last-Modified");
  /*printf("sizeStr:%s lastModifiedStr:%s\n",sizeStr.c_str(),
    lastModifiedStr.c_str());*/
  TF_RETURN_IF_ERROR(GetInt64Value(sizeStr, &(stat->length)));
  time_t lastModified;
  ParseRfc850Time(lastModifiedStr,lastModified);
  stat->mtime_nsec = lastModified*1000*1000;
  return Status::OK();
}

Status S3FileSystem::BucketExists(const string& bucket, bool* result) {
  string kStorageHost,kS3Key,kS3Secret;
  TF_RETURN_IF_ERROR(AuthProvider::GetHost(auth_provider_.get(), &kStorageHost));
  TF_RETURN_IF_ERROR(AuthProvider::GetKey(auth_provider_.get(), &kS3Key));
  TF_RETURN_IF_ERROR(AuthProvider::GetSecret(auth_provider_.get(), &kS3Secret));
  if (!result) {
    return errors::Internal("'result' cannot be nullptr.");
  }
  string signedValue;
  std::unique_ptr<HttpRequest> request(http_request_factory_->Create());
  TF_RETURN_IF_ERROR(request->Init());
  string resource = "/";
  TF_RETURN_IF_ERROR
    (request->SetUri(strings::StrCat
                     ("http://", kStorageHost,"/")));
  time_t now = time(0);
  string nowStr;
  string contentType="application/x-compressed-tar";
  TF_RETURN_IF_ERROR(GetRfc850Time(now,nowStr));
  TF_RETURN_IF_ERROR(request->AddHeader("Date",nowStr));
  TF_RETURN_IF_ERROR(request->AddHeader("Content-Type",contentType));
  string toSigned = strings::StrCat("GET\n\n",contentType,"\n",nowStr
                                    ,"\n",resource);

  TF_RETURN_IF_ERROR(ComputeS3Signed(kS3Secret,toSigned,signedValue));
  TF_RETURN_IF_ERROR(request->AddS3AuthBearerHeader(kS3Key,signedValue));
  //printf("signed:%s\n",signedValue.c_str());
  std::vector<char> output_buffer;
  TF_RETURN_IF_ERROR(request->SetResultBuffer(&output_buffer));
  TF_RETURN_WITH_CONTEXT_IF_ERROR(request->Send(), " when list  all buckets.");
  string output = string(output_buffer.data(),output_buffer.size());
  //printf("list buckets output:%s\n",output.c_str());
  tinyxml2::XMLDocument doc;
  doc.Parse(output.c_str());
  if(doc.Error()){
    return errors::Internal("xml:",output," Parse failure due to error:",
                            doc.ErrorName());
  }
  tinyxml2::XMLElement* resutlXml = 
    doc.FirstChildElement( "ListAllMyBucketsResult" );
  if(NULL == resutlXml){
    return errors::Internal("Unexpected response when list buckets",
                            " response xml not reconginize:",output);
  }
  *result = false;
  for(tinyxml2::XMLNode* bucketXML = 
        resutlXml->FirstChildElement( "Buckets" )->FirstChild();
      bucketXML;
      bucketXML = bucketXML->NextSibling()){
    if(NULL == bucketXML->FirstChildElement("Name")){
      return errors::Internal("can't find child elment Name from xml ",output);
    }
    string name = string(bucketXML->FirstChildElement("Name")->GetText());
    //printf("list bucket name :%s \n",name.c_str());
    if(name == bucket){
      *result = true;
      break;
    }
  }
  return Status::OK();
}

Status S3FileSystem::FolderExists(const string& dirname, bool* result) {
  if (!result) {
    return errors::Internal("'result' cannot be nullptr.");
  }
  std::vector<string> children;
  TF_RETURN_IF_ERROR(
      GetChildrenBounded(dirname, 1, &children, true /* recursively */,
                         true /* include_self_directory_marker */));
  *result = !children.empty();
  return Status::OK();
}

Status S3FileSystem::GetChildren(const string& dirname,
                                  std::vector<string>* result) {
  return GetChildrenBounded(dirname, UINT64_MAX, result,
                            false /* recursively */,
                            false /* include_self_directory_marker */);
}

Status S3FileSystem::GetMatchingPaths(const string& pattern,
                                       std::vector<string>* results) {
  results->clear();
  // Find the fixed prefix by looking for the first wildcard.
  const string& fixed_prefix =
      pattern.substr(0, pattern.find_first_of("*?[\\"));
  const string& dir = io::Dirname(fixed_prefix).ToString();
  if (dir.empty()) {
    return errors::InvalidArgument("A GCS pattern doesn't have a bucket name: ",
                                   pattern);
  }
  std::vector<string> all_files;
  TF_RETURN_IF_ERROR(
      GetChildrenBounded(dir, UINT64_MAX, &all_files, true /* recursively */,
                         false /* include_self_directory_marker */));

  const auto& files_and_folders = AddAllSubpaths(all_files);

  // Match all obtained paths to the input pattern.
  for (const auto& path : files_and_folders) {
    const string& full_path = io::JoinPath(dir, path);
    if (Env::Default()->MatchPath(full_path, pattern)) {
      results->push_back(full_path);
    }
  }
  return Status::OK();
}

Status S3FileSystem::GetChildrenBounded(const string& dirname,
                                         uint64 max_results,
                                         std::vector<string>* result,
                                         bool recursive,
                                         bool include_self_directory_marker) {
  string kStorageHost,kS3Key,kS3Secret;
  TF_RETURN_IF_ERROR(AuthProvider::GetHost(auth_provider_.get(), &kStorageHost));
  TF_RETURN_IF_ERROR(AuthProvider::GetKey(auth_provider_.get(), &kS3Key));
  TF_RETURN_IF_ERROR(AuthProvider::GetSecret(auth_provider_.get(), &kS3Secret));
  if (!result) {
    return errors::InvalidArgument("'result' cannot be null");
  }
  string bucket, object_prefix;
  TF_RETURN_IF_ERROR
    (ParseS3Path(MaybeAppendSlash(dirname), true, &bucket, &object_prefix));

  string nextMarker;
  uint64 retrieved_results = 0;
  //printf("dir name is :%s \n",dirname.c_str());
  while (true) {  // A loop over multiple result pages.
    std::vector<char> output_buffer;
    std::unique_ptr<HttpRequest> request(http_request_factory_->Create());
    TF_RETURN_IF_ERROR(request->Init());
    ///BUCKET-mygod/?prefix=nihao/&delimiter=/
    string resource = strings::StrCat("/", bucket,"/");
    auto uri = strings::StrCat("http://",kStorageHost, resource);
    uri = strings::StrCat(uri,"?prefix=",object_prefix);
    if (false == recursive) {
      uri = strings::StrCat(uri, "&delimiter=/");
    }
    //uri = strings::StrCat(uri, "&max-keys=", 1);
    if (max_results - retrieved_results < kGetChildrenDefaultPageSize) {
      uri = strings::StrCat(uri, "&max-keys=", max_results - retrieved_results);
    }
    if(false == nextMarker.empty()){
      uri = strings::StrCat(uri, "&marker=",nextMarker);
    }
    TF_RETURN_IF_ERROR(request->SetUri(uri));
    TF_RETURN_IF_ERROR(request->SetResultBuffer(&output_buffer));
    
    string signedValue;
    time_t now = time(0);
    string nowStr;
    string contentType="application/octet-stream";

    TF_RETURN_IF_ERROR(GetRfc850Time(now,nowStr));
    TF_RETURN_IF_ERROR(request->AddHeader("Date",nowStr));
    TF_RETURN_IF_ERROR(request->AddHeader("Content-Type",contentType));
    string toSigned = strings::StrCat("GET\n\n",contentType,"\n",nowStr
                                      ,"\n",resource);
    TF_RETURN_IF_ERROR(ComputeS3Signed(kS3Secret,toSigned,signedValue));
    TF_RETURN_IF_ERROR(request->AddS3AuthBearerHeader(kS3Key,signedValue));

    
    TF_RETURN_WITH_CONTEXT_IF_ERROR(request->Send(), " when reading ", dirname);
    nextMarker = "";
    string output = string(output_buffer.data(),output_buffer.size());
    //printf("get children output:%s\n",output.c_str());
    tinyxml2::XMLDocument doc;
    doc.Parse(output.c_str());
    if(doc.Error()){
      return errors::Internal("xml:",output," Parse failure due to error:",
                              doc.ErrorName());
    }
    tinyxml2::XMLElement * 
      listBucketResultXML =  doc.FirstChildElement( "ListBucketResult");
    if(NULL == listBucketResultXML){
      return errors::Internal
        ("Can't found root children ListBucketResult from xml ",output);
    }
    
    tinyxml2::XMLElement *  nextMarkerXML = 
      listBucketResultXML->FirstChildElement("NextMarker");
    if(nextMarkerXML!=NULL){
        nextMarker = nextMarkerXML->GetText();   
    }
    
    string lastObjectKey;
    for(tinyxml2::XMLNode* bucketXML = 
          listBucketResultXML->FirstChildElement("Contents");
        bucketXML;
        bucketXML = bucketXML->NextSibling()){
      tinyxml2::XMLElement * keyXML = bucketXML->FirstChildElement("Key");
      if(NULL == keyXML){
        return errors::Internal
          ("Can't found Key children ListBucketResult from xml ",output);
      }
      string objectname = keyXML->GetText();
      lastObjectKey = objectname;
      StringPiece relative_path(objectname);
      if (!relative_path.Consume(object_prefix)) {
        return errors::Internal
          (strings::StrCat("Unexpected response: the returned file name ", 
                           objectname,
                           " doesn't match the prefix ", object_prefix));
      }
      if (!relative_path.empty() || include_self_directory_marker) {
        result->emplace_back(relative_path.ToString());
      }
      if (++retrieved_results >= max_results) {
        return Status::OK();
      }
    }
    for(tinyxml2::XMLNode* bucketXML = 
          listBucketResultXML->FirstChildElement("CommonPrefixes");
        bucketXML;
        bucketXML = bucketXML->NextSibling()){

      tinyxml2::XMLElement * keyXML = bucketXML->FirstChildElement("Prefix");
      if(NULL == keyXML){
        return errors::Internal
          ("Can't found Prefix children ListBucketResult from xml ",output);
      }
      string objectname = keyXML->GetText();
      lastObjectKey = objectname;
      StringPiece relative_path(objectname);
      if (!relative_path.Consume(object_prefix)) {
        return errors::Internal
          (strings::StrCat("Unexpected response: the returned file name ", 
                           objectname,
                           " doesn't match the prefix ", object_prefix));
      }
      if (!relative_path.empty() || include_self_directory_marker) {
        result->emplace_back(relative_path.ToString());
      }
      if (++retrieved_results >= max_results) {
        return Status::OK();
      }
    }
    if(nextMarker.empty()){
      nextMarker = lastObjectKey;
    }
    if(nextMarker.empty()){
      return Status::OK();
    }
  }//end while true
}

Status S3FileSystem::Stat(const string& fname, FileStatistics* stat) {
  if (!stat) {
    return errors::Internal("'stat' cannot be nullptr.");
  }
  string bucket, object;
  TF_RETURN_IF_ERROR(ParseS3Path(fname, true, &bucket, &object));
  if (object.empty()) {
    bool is_bucket;
    TF_RETURN_IF_ERROR(BucketExists(bucket, &is_bucket));
    if (is_bucket) {
      *stat = DIRECTORY_STAT;
      return Status::OK();
    }
    return errors::NotFound("The specified bucket ", fname, " was not found.");
  }

  const Status status = StatForObject(bucket, object, stat);
  if (status.ok()) {
    return Status::OK();
  }
  if (status.code() != errors::Code::NOT_FOUND) {
    return status;
  }
  bool is_folder;
  TF_RETURN_IF_ERROR(FolderExists(fname, &is_folder));
  if (is_folder) {
    *stat = DIRECTORY_STAT;
    return Status::OK();
  }
  return errors::NotFound("The specified path ", fname, " was not found.");
}

Status S3FileSystem::DeleteFile(const string& fname) {
  string kStorageHost,kS3Key,kS3Secret;
  TF_RETURN_IF_ERROR(AuthProvider::GetHost(auth_provider_.get(), &kStorageHost));
  TF_RETURN_IF_ERROR(AuthProvider::GetKey(auth_provider_.get(), &kS3Key));
  TF_RETURN_IF_ERROR(AuthProvider::GetSecret(auth_provider_.get(), &kS3Secret));

  string bucket, object;
  TF_RETURN_IF_ERROR(ParseS3Path(fname, false, &bucket, &object));
  string signedValue;
  
  std::vector<char> output_buffer;

  std::unique_ptr<HttpRequest> request(http_request_factory_->Create());
  TF_RETURN_IF_ERROR(request->Init());

  string resource = 
    strings::StrCat("/", bucket,"/",
                    object);
  TF_RETURN_IF_ERROR
    (request->SetUri(strings::StrCat
                     ("http://", kStorageHost,resource)));
  time_t now = time(0);
  string nowStr;
  string contentType="application/octet-stream";
  TF_RETURN_IF_ERROR(GetRfc850Time(now,nowStr));
  TF_RETURN_IF_ERROR(request->AddHeader("Date",nowStr));
  TF_RETURN_IF_ERROR(request->AddHeader("Content-Type",contentType));
  string toSigned = strings::StrCat("DELETE\n\n",contentType,"\n",nowStr
                                    ,"\n",resource);
  TF_RETURN_IF_ERROR(ComputeS3Signed(kS3Secret,toSigned,signedValue));
  TF_RETURN_IF_ERROR(request->AddS3AuthBearerHeader(kS3Key,signedValue));

  TF_RETURN_IF_ERROR
    (request->SetDeleteRequest());
  TF_RETURN_WITH_CONTEXT_IF_ERROR(request->Send(), " when deleting ",fname);
  //string output = string(output_buffer.data(),output_buffer.size());
  //printf("delete object output:%s\n",output.c_str());
  
  return Status::OK();
}

Status S3FileSystem::CreateDir(const string& dirname) {
  string bucket, object;
  TF_RETURN_IF_ERROR(ParseS3Path(dirname, true, &bucket, &object));
  if (object.empty()) {
    bool is_bucket;
    TF_RETURN_IF_ERROR(BucketExists(bucket, &is_bucket));
    return is_bucket ? Status::OK()
                     : errors::NotFound("The specified bucket ", dirname,
                                        " was not found.");
  }
  // Create a zero-length directory marker object.
  std::unique_ptr<WritableFile> file;
  TF_RETURN_IF_ERROR(NewWritableFile(MaybeAppendSlash(dirname), &file));
  TF_RETURN_IF_ERROR(file->Close());
  return Status::OK();
}

// Checks that the directory is empty (i.e no objects with this prefix exist).
// If it is, does nothing, because directories are not entities in GCS.
Status S3FileSystem::DeleteDir(const string& dirname) {
  std::vector<string> children;
  // A directory is considered empty either if there are no matching objects
  // with the corresponding name prefix or if there is exactly one matching
  // object and it is the directory marker. Therefore we need to retrieve
  // at most two children for the prefix to detect if a directory is empty.
  TF_RETURN_IF_ERROR(
      GetChildrenBounded(dirname, 2, &children, true /* recursively */,
                         true /* include_self_directory_marker */));

  if (children.size() > 1 || (children.size() == 1 && !children[0].empty())) {
    return errors::FailedPrecondition("Cannot delete a non-empty directory.");
  }
  if (children.size() == 1 && children[0].empty()) {
    // This is the directory marker object. Delete it.
    return DeleteFile(MaybeAppendSlash(dirname));
  }
  return Status::OK();
}

Status S3FileSystem::GetFileSize(const string& fname, uint64* file_size) {
  if (!file_size) {
    return errors::Internal("'file_size' cannot be nullptr.");
  }

  // Only validate the name.
  string bucket, object;
  TF_RETURN_IF_ERROR(ParseS3Path(fname, false, &bucket, &object));

  FileStatistics stat;
  TF_RETURN_IF_ERROR(Stat(fname, &stat));
  *file_size = stat.length;
  return Status::OK();
}

Status S3FileSystem::RenameFile(const string& src, const string& target) {
  if (!IsDirectory(src).ok()) {
    return RenameObject(src, target);
  }
  // Rename all individual objects in the directory one by one.
  std::vector<string> children;
  TF_RETURN_IF_ERROR(
      GetChildrenBounded(src, UINT64_MAX, &children, true /* recursively */,
                         true /* include_self_directory_marker */));
  for (const string& subpath : children) {
    TF_RETURN_IF_ERROR(
        RenameObject(JoinS3Path(src, subpath), JoinS3Path(target, subpath)));
  }
  return Status::OK();
}

// Uses a GCS API command to copy the object and then deletes the old one.
Status S3FileSystem::RenameObject(const string& src, const string& target) {
  string kStorageHost,kS3Key,kS3Secret;
  TF_RETURN_IF_ERROR(AuthProvider::GetHost(auth_provider_.get(), &kStorageHost));
  TF_RETURN_IF_ERROR(AuthProvider::GetKey(auth_provider_.get(), &kS3Key));
  TF_RETURN_IF_ERROR(AuthProvider::GetSecret(auth_provider_.get(), &kS3Secret));
  string src_bucket, src_object, target_bucket, target_object;
  TF_RETURN_IF_ERROR(ParseS3Path(src, false, &src_bucket, &src_object));
  TF_RETURN_IF_ERROR(
      ParseS3Path(target, false, &target_bucket, &target_object));
  
  string signedValue;
  
  std::vector<char> output_buffer;

  std::unique_ptr<HttpRequest> request(http_request_factory_->Create());
  TF_RETURN_IF_ERROR(request->Init());

  string resource = 
    strings::StrCat("/", target_bucket,"/",
                    target_object);
  TF_RETURN_IF_ERROR
    (request->SetUri(strings::StrCat
                     ("http://", kStorageHost,resource)));
  time_t now = time(0);
  string nowStr;
  string contentType="application/octet-stream";
  TF_RETURN_IF_ERROR(GetRfc850Time(now,nowStr));
  TF_RETURN_IF_ERROR(request->AddHeader("Date",nowStr));
  TF_RETURN_IF_ERROR(request->AddHeader("x-amz-copy-source", 
                                        strings::StrCat("/",src_bucket,
                                                        "/",src_object)));
  TF_RETURN_IF_ERROR(request->AddHeader("Content-Type",contentType));
  string toSigned = strings::StrCat("PUT\n\n",contentType,"\n",nowStr
                                    ,"\nx-amz-copy-source:/",
                                    src_bucket,"/",src_object,"\n",
                                    resource);
  TF_RETURN_IF_ERROR(ComputeS3Signed(kS3Secret,toSigned,signedValue));
  TF_RETURN_IF_ERROR(request->AddS3AuthBearerHeader(kS3Key,signedValue));

  TF_RETURN_IF_ERROR
    (request->SetPutRequest());
  TF_RETURN_WITH_CONTEXT_IF_ERROR(request->Send(), 
                                  " when copy /",src_bucket,"/",src_object,
                                  " to /",target_bucket,"/",target_object);
  //string output = string(output_buffer.data(),output_buffer.size());
  //printf("copy object output:%s\n",output.c_str());
  TF_RETURN_IF_ERROR(DeleteFile(src));
  return Status::OK();
}

Status S3FileSystem::IsDirectory(const string& fname) {
  string bucket, object;
  TF_RETURN_IF_ERROR(ParseS3Path(fname, true, &bucket, &object));
  if (object.empty()) {
    bool is_bucket;
    TF_RETURN_IF_ERROR(BucketExists(bucket, &is_bucket));
    if (is_bucket) {
      return Status::OK();
    }
    return errors::NotFound("The specified bucket gs://", bucket,
                            " was not found.");
  }
  bool is_folder;
  TF_RETURN_IF_ERROR(FolderExists(fname, &is_folder));
  if (is_folder) {
    return Status::OK();
  }
  bool is_object;
  TF_RETURN_IF_ERROR(ObjectExists(bucket, object, &is_object));
  if (is_object) {
    return errors::FailedPrecondition("The specified path ", fname,
                                      " is not a directory.");
  }
  return errors::NotFound("The specified path ", fname, " was not found.");
}

Status S3FileSystem::DeleteRecursively(const string& dirname,
                                        int64* undeleted_files,
                                        int64* undeleted_dirs) {
  if (!undeleted_files || !undeleted_dirs) {
    return errors::Internal(
        "'undeleted_files' and 'undeleted_dirs' cannot be nullptr.");
  }
  *undeleted_files = 0;
  *undeleted_dirs = 0;
  if (!IsDirectory(dirname).ok()) {
    *undeleted_dirs = 1;
    return Status(
        error::NOT_FOUND,
        strings::StrCat(dirname, " doesn't exist or not a directory."));
  }
  std::vector<string> all_objects;
  // Get all children in the directory recursively.
  TF_RETURN_IF_ERROR(GetChildrenBounded(
      dirname, UINT64_MAX, &all_objects, true /* recursively */,
      true /* include_self_directory_marker */));
  for (const string& object : all_objects) {
    const string& full_path = JoinS3Path(dirname, object);
    // Delete all objects including directory markers for subfolders.
    if (!DeleteFile(full_path).ok()) {
      if (IsDirectory(full_path).ok()) {
        // The object is a directory marker.
        (*undeleted_dirs)++;
      } else {
        (*undeleted_files)++;
      }
    }
  }
  return Status::OK();
}

REGISTER_FILE_SYSTEM("s3", RetryingS3FileSystem);
}
}  // namespace tensorflow
