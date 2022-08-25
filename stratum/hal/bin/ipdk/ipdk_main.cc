// Copyright 2018-2019 Barefoot Networks, Inc.
// Copyright 2020-present Open Networking Foundation
// Copyright 2022 Intel Corporation
// SPDX-License-Identifier: Apache-2.0

#include "stratum/hal/bin/ipdk/ipdk_main.h"

#include "gflags/gflags.h"
#include "stratum/glue/init_google.h"
#include "stratum/glue/logging.h"
#include "stratum/hal/lib/ipdk/ipdk_chassis_manager.h"
#include "stratum/hal/lib/ipdk/ipdk_hal.h"
#include "stratum/hal/lib/ipdk/ipdk_node.h"
#include "stratum/hal/lib/ipdk/ipdk_switch.h"
#include "stratum/hal/lib/phal/phal_sim.h"
#include "stratum/hal/lib/tdi/tdi_action_profile_manager.h"
#include "stratum/hal/lib/tdi/tdi_counter_manager.h"
#include "stratum/hal/lib/tdi/tdi_pre_manager.h"
#include "stratum/hal/lib/tdi/tdi_sde_wrapper.h"
#include "stratum/hal/lib/tdi/tdi_table_manager.h"
#include "stratum/lib/security/auth_policy_checker.h"

DEFINE_string(ipdk_sde_install, "/usr",
              "Absolute path to the directory where the SDE is installed");
DEFINE_bool(ipdk_infrap4d_background, false,
            "Run infrap4d in the background with no interactive features");
// TODO(dfoster): Default value for IPDK?
DEFINE_string(ipdk_infrap4d_cfg, "stratum/hal/bin/ipdk/tofino_skip_p4.conf",
              "Path to the infrap4d json config file");

namespace stratum {
namespace hal {
namespace barefoot {

::util::Status IpdkMain(int argc, char* argv[]) {
  InitGoogle(argv[0], &argc, &argv, true);
  InitStratumLogging();

  // TODO(antonin): The SDE expects 0-based device ids, so we instantiate
  // components with "device_id" instead of "node_id".
  const int device_id = 0;

  auto sde_wrapper = TdiSdeWrapper::CreateSingleton();

  RETURN_IF_ERROR(sde_wrapper->InitializeSde(
      FLAGS_ipdk_sde_install, FLAGS_ipdk_infrap4d_cfg,
      FLAGS_ipdk_infrap4d_background));

  ASSIGN_OR_RETURN(bool is_sw_model,
                   sde_wrapper->IsSoftwareModel(device_id));
  const OperationMode mode =
      is_sw_model ? OPERATION_MODE_SIM : OPERATION_MODE_STANDALONE;

  VLOG(1) << "Detected is_sw_model: " << is_sw_model;
  VLOG(1) << "SDE version: " << sde_wrapper->GetSdeVersion();
  VLOG(1) << "Switch SKU: " << sde_wrapper->GetBfChipType(device_id);

  auto table_manager =
      TdiTableManager::CreateInstance(mode, sde_wrapper, device_id);

  auto action_profile_manager =
      TdiActionProfileManager::CreateInstance(sde_wrapper, device_id);

  auto packetio_manager =
      TdiPacketioManager::CreateInstance(sde_wrapper, device_id);

  auto pre_manager =
      TdiPreManager::CreateInstance(sde_wrapper, device_id);

  auto counter_manager =
      TdiCounterManager::CreateInstance(sde_wrapper, device_id);

  auto ipdk_node = IpdkNode::CreateInstance(
      table_manager.get(), action_profile_manager.get(),
      packetio_manager.get(), pre_manager.get(),
      counter_manager.get(), sde_wrapper, device_id);

  PhalInterface* phal = PhalSim::CreateSingleton();

  std::map<int, IpdkNode*> device_id_to_ipdk_node = {
      {device_id, ipdk_node.get()},
  };

  auto chassis_manager =
      IpdkChassisManager::CreateInstance(mode, phal, sde_wrapper);

  auto ipdk_switch = IpdkSwitch::CreateInstance(
      phal, chassis_manager.get(), sde_wrapper, device_id_to_ipdk_node);

  // Create the 'Hal' class instance.
  auto auth_policy_checker = AuthPolicyChecker::CreateInstance();

  auto* hal = IpdkHal::CreateSingleton(
      stratum::hal::OPERATION_MODE_STANDALONE, ipdk_switch.get(),
      auth_policy_checker.get());
  CHECK_RETURN_IF_FALSE(hal) << "Failed to create the Stratum Hal instance.";

  // Set up P4 runtime servers.
  ::util::Status status = hal->Setup();
  if (!status.ok()) {
    LOG(ERROR)
        << "Error when setting up Stratum HAL (but we will continue running): "
        << status.error_message();
  }

  // Start serving RPCs.
  RETURN_IF_ERROR(hal->Run());  // blocking

  LOG(INFO) << "See you later!";
  return ::util::OkStatus();
}

}  // namespace barefoot
}  // namespace hal
}  // namespace stratum

int main(int argc, char* argv[]) {
  return stratum::hal::barefoot::IpdkMain(argc, argv).error_code();
}
