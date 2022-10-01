// Copyright 2022 Intel Corporation
// SPDX-License-Identifier: Apache-2.0

#ifndef STRATUM_HAL_BIN_TDI_TDI_MAIN_H_
#define STRATUM_HAL_BIN_TDI_TDI_MAIN_H_

#include "stratum/glue/status/status.h"

namespace stratum {
namespace hal {
namespace tdix {

::util::Status TdiMain(int argc, char* argv[]);

}  // namespace tdix
}  // namespace hal
}  // namespace stratum

#endif  // STRATUM_HAL_BIN_TDI_TDI_MAIN_H_
