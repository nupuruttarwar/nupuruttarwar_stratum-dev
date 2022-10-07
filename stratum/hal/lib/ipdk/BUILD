# Bazel build file for hal/lib/ipdk

# Copyright 2018 Google LLC
# Copyright 2018-present Open Networking Foundation
# Copyright 2022 Intel Corporation
# SPDX-License-Identifier: Apache-2.0

load(
    "//bazel:rules.bzl",
    "HOST_ARCHES",
    "STRATUM_INTERNAL",
    "stratum_cc_library",
    "stratum_cc_test",
)

licenses(["notice"])  # Apache v2

package(
    default_visibility = STRATUM_INTERNAL,
)

stratum_cc_library(
    name = "ipdk_chassis_manager",
    srcs = ["ipdk_chassis_manager.cc"],
    hdrs = ["ipdk_chassis_manager.h"],
    defines = ["P4OVS_CHANGES", "IPDK_TARGET"],
    deps = [
        "//stratum/glue/gtl:map_util",
        "//stratum/glue/status",
        "//stratum/glue/status:statusor",
        "//stratum/glue:integral_types",
        "//stratum/glue:logging",
        "//stratum/hal/lib/common:common_cc_proto",
        "//stratum/hal/lib/common:constants",
        "//stratum/hal/lib/common:phal_interface",
        "//stratum/hal/lib/common:switch_interface",
        "//stratum/hal/lib/common:utils",
        "//stratum/hal/lib/common:writer_interface",
        "//stratum/hal/lib/tdi:tdi_sde_interface",
        "//stratum/lib:constants",
        "//stratum/lib:macros",
        "//stratum/lib:utils",
        "//stratum/public/lib:error",
        "@com_github_google_glog//:glog",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/types:optional",
        "@com_google_protobuf//:protobuf",
    ],
)

stratum_cc_library(
    name = "ipdk_switch",
    srcs = ["ipdk_switch.cc"],
    hdrs = ["ipdk_switch.h"],
    defines = ["P4OVS_CHANGES", "IPDK_TARGET"],
    deps = [
        ":ipdk_chassis_manager",
        "//stratum/glue/gtl:map_util",
        "//stratum/glue/status:status_macros",
        "//stratum/glue:integral_types",
        "//stratum/glue:logging",
        "//stratum/hal/lib/tdi:tdi_cc_proto",
        "//stratum/hal/lib/tdi:tdi_pipeline_utils",
        "//stratum/hal/lib/tdi:tdi_sde_interface",
        "//stratum/hal/lib/ipdk:ipdk_node",
        "//stratum/hal/lib/tdi:utils",
        "//stratum/hal/lib/common:phal_interface",
        "//stratum/hal/lib/common:switch_interface",
        "//stratum/lib:constants",
        "//stratum/lib:macros",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/synchronization",
        "@com_google_googleapis//google/rpc:status_cc_proto",
    ],
)

stratum_cc_test(
    name = "ipdk_chassis_manager_test",
    srcs = ["ipdk_chassis_manager_test.cc"],
    defines = ["P4OVS_CHANGES", "IPDK_TARGET"],
    deps = [
        ":ipdk_chassis_manager",
        ":test_main",
        "//stratum/glue/gtl:map_util",
        "//stratum/glue/status",
        "//stratum/glue/status:status_test_util",
        "//stratum/glue/status:statusor",
        "//stratum/glue:integral_types",
        "//stratum/hal/lib/common:common_cc_proto",
        "//stratum/hal/lib/common:constants",
        "//stratum/hal/lib/common:phal_mock",
        "//stratum/hal/lib/common:utils",
        "//stratum/hal/lib/common:writer_interface",
        "//stratum/hal/lib/common:writer_mock",
        "//stratum/hal/lib/tdi:tdi_sde_mock",
        "//stratum/lib:utils",
        "//stratum/lib/test_utils:matchers",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/time",
        "@com_google_googletest//:gtest",
    ],
)

stratum_cc_library(
    name = "ipdk_chassis_manager_mock",
    testonly = 1,
    hdrs = ["ipdk_chassis_manager_mock.h"],
    defines = ["P4OVS_CHANGES", "IPDK_TARGET"],
    arches = HOST_ARCHES,
    deps = [
        ":ipdk_chassis_manager",
        "@com_google_googletest//:gtest",
    ],
)

stratum_cc_library(
    name = "test_main",
    testonly = 1,
    srcs = ["test_main.cc"],
    defines = ["P4OVS_CHANGES", "IPDK_TARGET"],
    deps = [
        "//stratum/glue:init_google",
        "//stratum/glue:logging",
        "@com_github_google_glog//:glog",
        "@com_google_googletest//:gtest",
    ],
)

stratum_cc_library(
    name = "ipdk_node",
    srcs = ["ipdk_node.cc"],
    hdrs = ["ipdk_node.h"],
    defines = ["P4OVS_CHANGES", "IPDK_TARGET"],
    deps = [
        "//stratum/glue/status:status_macros",
        "//stratum/glue:integral_types",
        "//stratum/glue:logging",
        "//stratum/hal/lib/common:common_cc_proto",
        "//stratum/hal/lib/common:proto_oneof_writer_wrapper",
        "//stratum/hal/lib/common:writer_interface",
        "//stratum/hal/lib/tdi:tdi_action_profile_manager",
        "//stratum/hal/lib/tdi:tdi_cc_proto",
        "//stratum/hal/lib/tdi:tdi_counter_manager",
        "//stratum/hal/lib/tdi:tdi_packetio_manager",
        "//stratum/hal/lib/tdi:tdi_pipeline_utils",
        "//stratum/hal/lib/tdi:tdi_pre_manager",
        "//stratum/hal/lib/tdi:tdi_table_manager",
        "//stratum/lib:constants",
        "//stratum/lib:macros",
        "//stratum/lib:utils",
        "//stratum/public/proto:error_cc_proto",
        "@com_github_p4lang_p4runtime//:p4runtime_cc_grpc",
        "@com_google_googleapis//google/rpc:status_cc_proto",
    ],
)

stratum_cc_library(
    name = "ipdk_node_mock",
    testonly = 1,
    hdrs = ["ipdk_node_mock.h"],
    arches = HOST_ARCHES,
    defines = ["P4OVS_CHANGES", "IPDK_TARGET"],
    deps = [
        ":ipdk_node",
        "@com_google_googletest//:gtest",
    ],
)

# Not yet integrated or tested.
stratum_cc_library(
    name = "ipdk_hal",
    srcs = ["ipdk_hal.cc"],
    hdrs = ["ipdk_hal.h"],
    defines = ["P4OVS_CHANGES", "IPDK_TARGET"],
    deps = [
        "//stratum/glue:logging",
        "//stratum/hal/lib/common:common_cc_proto",
        "//stratum/hal/lib/common:config_monitoring_service",
        "//stratum/hal/lib/common:error_buffer",
        "//stratum/hal/lib/common:p4_service",
        "//stratum/hal/lib/common:switch_interface",
        "//stratum/lib/security:auth_policy_checker",
        "//stratum/lib:constants",
        "//stratum/lib:macros",
        "//stratum/lib:utils",
        "@com_github_google_glog//:glog",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
        "@com_github_grpc_grpc//:grpc++",
    ],
)

stratum_cc_test(
    name = "ipdk_hal_test",
    srcs = [
        "ipdk_hal_test.cc",
    ],
    defines = ["P4OVS_CHANGES", "IPDK_TARGET"],
    deps = [
        ":ipdk_hal",
        ":test_main",
        "//stratum/glue/net_util:ports",
        "//stratum/glue/status:status_test_util",
        "//stratum/hal/lib/common:switch_mock",
        "//stratum/lib/security:auth_policy_checker_mock",
        "//stratum/lib/test_utils:matchers",
        "//stratum/lib:utils",
        "//stratum/public/lib:error",
        "@com_github_google_glog//:glog",
        "@com_google_absl//absl/strings",
        "@com_google_googletest//:gtest",
    ],
)

stratum_cc_test(
    name = "ipdk_switch_test",
    srcs = [
        "ipdk_switch_test.cc",
    ],
    defines = ["P4OVS_CHANGES", "IPDK_TARGET"],
    deps = [
        ":ipdk_chassis_manager_mock",
        ":ipdk_switch",
        ":test_main",
        "//stratum/glue/status:status_test_util",
        "//stratum/hal/lib/common:phal_mock",
        "//stratum/hal/lib/common:switch_interface",
        "//stratum/hal/lib/common:writer_mock",
        "//stratum/hal/lib/ipdk:ipdk_node_mock",
        "//stratum/hal/lib/p4:p4_table_mapper_mock",
        "//stratum/hal/lib/tdi:tdi_sde_mock",
        "//stratum/lib/channel:channel_mock",
        "//stratum/lib:utils",
        "//stratum/lib/test_utils:matchers",
        "@com_google_absl//absl/memory",
        "@com_google_googletest//:gtest",
    ],
)
