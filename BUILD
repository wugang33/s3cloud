# Description:
# Cloud file system implementation.

package(
    default_visibility = ["//visibility:private"],
)

licenses(["notice"])  # Apache 2.0

load(
    "//tensorflow:tensorflow.bzl",
    "tf_cc_test",
)

filegroup(
    name = "all_files",
    srcs = glob(
        ["**/*"],
        exclude = [
            "**/METADATA",
            "**/OWNERS",
        ],
    ),
    visibility = ["//tensorflow:__subpackages__"],
)

cc_library(
    name = "s3_file_system",
    srcs = [
        "s3_file_system.cc",
    ],
    hdrs = [
        "s3_file_system.h",
    ],
    linkstatic = 1,  # Needed since alwayslink is broken in bazel b/27630669
    visibility = ["//visibility:public"],
    deps = [
        ":s3_auth_provider",
        ":http_request",
        ":retrying_file_system",
        ":time_util",
        ":tinyxml2",
        "//tensorflow/core:framework_headers_lib",
        "//tensorflow/core:lib_internal",
        "@jsoncpp_git//:jsoncpp",
    ],
    alwayslink = 1,
)
cc_library(
    name = "tinyxml2",
    srcs = ["tinyxml2.cpp"],
    hdrs = ["tinyxml2.h"],
    visibility = ["//tensorflow:__subpackages__"],
    deps = [
    ],
)

cc_library(
    name = "http_request",
    srcs = ["http_request.cc"],
    hdrs = ["http_request.h"],
    visibility = ["//tensorflow:__subpackages__"],
    deps = [
        "//tensorflow/core:framework_headers_lib",
        "//tensorflow/core:lib_internal",
        "@curl//:curl",
    ],
)

cc_library(
    name = "http_request_fake",
    testonly = 1,
    hdrs = [
        "http_request_fake.h",
    ],
    visibility = ["//tensorflow:__subpackages__"],
    deps = [
        ":http_request",
        "//tensorflow/core:lib",
        "//tensorflow/core:lib_internal",
        "//tensorflow/core:test",
        "@curl//:curl",
    ],
)

cc_library(
    name = "s3_auth_provider",
    srcs = [
        "s3_auth_provider.cc",
    ],
    hdrs = [
        "auth_provider.h",
        "s3_auth_provider.h",
        "ini.h"
    ],
    visibility = ["//tensorflow:__subpackages__"],
    deps = [
        "//tensorflow/core:lib",
        "//tensorflow/core:lib_internal",
        "@jsoncpp_git//:jsoncpp",
    ],
)

cc_library(
    name = "retrying_file_system",
    srcs = [
        "retrying_file_system.cc",
    ],
    hdrs = [
        "retrying_file_system.h",
    ],
    deps = [
        "//tensorflow/core:framework_headers_lib",
        "//tensorflow/core:lib_internal",
    ],
)

cc_library(
    name = "time_util",
    srcs = [
        "time_util.cc",
    ],
    hdrs = [
        "time_util.h",
    ],
    deps = [
        "//tensorflow/core:framework_headers_lib",
        "//tensorflow/core:lib_internal",
        "@curl//:curl",
    ],
)

tf_cc_test(
    name = "s3_file_system_test",
    size = "small",
    srcs = ["s3_file_system_test.cc"],
    deps = [
        ":s3_file_system",
        ":http_request_fake",
        "//tensorflow/core:test",
        "//tensorflow/core:test_main",
    ],
)

tf_cc_test(
    name = "http_request_test",
    size = "small",
    srcs = ["http_request_test.cc"],
    deps = [
        ":http_request",
        "//tensorflow/core:lib",
        "//tensorflow/core:test",
        "//tensorflow/core:test_main",
    ],
)

tf_cc_test(
    name = "oauth_client_test",
    size = "small",
    srcs = ["oauth_client_test.cc"],
    data = [
        "testdata/service_account_credentials.json",
        "testdata/service_account_public_key.txt",
    ],
    deps = [
        ":http_request_fake",
        ":oauth_client",
        "//tensorflow/core:lib",
        "//tensorflow/core:lib_internal",
        "//tensorflow/core:test",
        "//tensorflow/core:test_main",
        "@boringssl//:crypto",
    ],
)

tf_cc_test(
    name = "s3_auth_provider_test",
    size = "small",
    srcs = ["s3_auth_provider_test.cc"],
    deps = [
        ":s3_auth_provider",
        ":http_request_fake",
        "//tensorflow/core:lib",
        "//tensorflow/core:lib_internal",
        "//tensorflow/core:test",
        "//tensorflow/core:test_main",
    ],
)

tf_cc_test(
    name = "retrying_file_system_test",
    size = "small",
    srcs = ["retrying_file_system_test.cc"],
    deps = [
        ":retrying_file_system",
        "//tensorflow/core:lib",
        "//tensorflow/core:lib_internal",
        "//tensorflow/core:test",
        "//tensorflow/core:test_main",
    ],
)

tf_cc_test(
    name = "time_util_test",
    size = "small",
    srcs = ["time_util_test.cc"],
    deps = [
        ":time_util",
        "//tensorflow/core:test",
        "//tensorflow/core:test_main",
    ],
)
