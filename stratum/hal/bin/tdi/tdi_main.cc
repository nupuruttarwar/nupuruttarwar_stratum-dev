// Copyright 2018-2019 Barefoot Networks, Inc.
// Copyright 2020-present Open Networking Foundation
// Copyright 2022 Intel Corporation
// SPDX-License-Identifier: Apache-2.0

#include "gflags/gflags.h"
#include "stratum/glue/init_google.h"
#include "stratum/glue/logging.h"
#include "stratum/hal/lib/phal/phal_sim.h"
#include "stratum/hal/lib/tdi/tdi_chassis_manager.h"
#include "stratum/hal/lib/tdi/tdi_sde_wrapper.h"
#include "stratum/hal/lib/tdi/tdi_action_profile_manager.h"
#include "stratum/hal/lib/tdi/tdi_counter_manager.h"
#include "stratum/hal/lib/tdi/tdi_hal.h"
#include "stratum/hal/lib/tdi/tdi_node.h"
#include "stratum/hal/lib/tdi/tdi_pre_manager.h"
#include "stratum/hal/lib/tdi/tdi_switch.h"
#include "stratum/hal/lib/tdi/tdi_table_manager.h"
#include "stratum/lib/security/auth_policy_checker.h"

DEFINE_string(tdi_sde_install, "/opt/sde",
              "Absolute path to the directory where the SDE is installed");
DEFINE_bool(tdi_switchd_background, false,
            "Run switch daemon in the background with no interactive features");
// TODO: Rename default configuration file.
DEFINE_string(tdi_switchd_cfg, "stratum/hal/bin/tdi/tofino_skip_p4.conf",
              "Path to the switch daemon json config file");

namespace stratum {
namespace hal {
namespace barefoot {

::util::Status TdiMain(int argc, char* argv[]) {
  InitGoogle(argv[0], &argc, &argv, true);
  InitStratumLogging();

  // TODO(antonin): The SDE expects 0-based device ids, so we instantiate
  // components with "device_id" instead of "node_id".
  const int device_id = 0;

  auto tdi_sde_wrapper = TdiSdeWrapper::CreateSingleton();

  RETURN_IF_ERROR(tdi_sde_wrapper->InitializeSde(
      FLAGS_tdi_sde_install, FLAGS_tdi_switchd_cfg, FLAGS_tdi_switchd_background));

  ASSIGN_OR_RETURN(bool is_sw_model,
                   tdi_sde_wrapper->IsSoftwareModel(device_id));
  const OperationMode mode =
      is_sw_model ? OPERATION_MODE_SIM : OPERATION_MODE_STANDALONE;

  VLOG(1) << "Detected is_sw_model: " << is_sw_model;
  VLOG(1) << "SDE version: " << tdi_sde_wrapper->GetSdeVersion();
  VLOG(1) << "Switch SKU: " << tdi_sde_wrapper->GetBfChipType(device_id);

  auto tdi_table_manager =
      TdiTableManager::CreateInstance(mode, tdi_sde_wrapper, device_id);

  auto tdi_action_profile_manager =
      TdiActionProfileManager::CreateInstance(tdi_sde_wrapper, device_id);

  auto tdi_packetio_manger =
      TdiPacketioManager::CreateInstance(tdi_sde_wrapper, device_id);

  auto tdi_pre_manager =
      TdiPreManager::CreateInstance(tdi_sde_wrapper, device_id);

  auto tdi_counter_manager =
      TdiCounterManager::CreateInstance(tdi_sde_wrapper, device_id);

  auto tdi_node = TdiNode::CreateInstance(
      tdi_table_manager.get(), tdi_action_profile_manager.get(),
      tdi_packetio_manger.get(), tdi_pre_manager.get(),
      tdi_counter_manager.get(), tdi_sde_wrapper, device_id);

  PhalInterface* phal = PhalSim::CreateSingleton();

  std::map<int, TdiNode*> device_id_to_tdi_node = {
      {device_id, tdi_node.get()},
  };

  auto tdi_chassis_manager =
      TdiChassisManager::CreateInstance(mode, phal, tdi_sde_wrapper);

  auto tdi_switch =
      TdiSwitch::CreateInstance(phal, tdi_chassis_manager.get(),
				tdi_sde_wrapper, device_id_to_tdi_node);

  // Create the 'Hal' class instance.
  auto auth_policy_checker = AuthPolicyChecker::CreateInstance();

  auto* hal = TdiHal::CreateSingleton(
      stratum::hal::OPERATION_MODE_STANDALONE, tdi_switch.get(),
      auth_policy_checker.get());
  RET_CHECK(hal) << "Failed to create the Stratum Hal instance.";

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
  return stratum::hal::barefoot::TdiMain(argc, argv).error_code();
}
