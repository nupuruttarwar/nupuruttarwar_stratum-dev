// Copyright 2022 Intel Corporation
// SPDX-License-Identifier: Apache-2.0

#ifndef STRATUM_HAL_LIB_IPDK_IPDK_MAIN_H_
#define STRATUM_HAL_LIB_IPDK_IPDK_MAIN_H_

#include "stratum/glue/status/status.h"

namespace stratum {
namespace hal {
namespace barefoot {

::util::Status IpdkMain(int argc, char* argv[]);

}  // namespace barefoot
}  // namespace hal
}  // namespace stratum

#endif  // STRATUM_HAL_LIB_IPDK_IPDK_MAIN_H_
