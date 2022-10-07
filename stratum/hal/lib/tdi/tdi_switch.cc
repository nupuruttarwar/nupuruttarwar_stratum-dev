// Copyright 2020-present Open Networking Foundation
// Copyright 2022 Intel Corporation
// SPDX-License-Identifier: Apache-2.0

#include "stratum/hal/lib/tdi/tdi_switch.h"

#include <algorithm>
#include <map>
#include <vector>

#include "absl/memory/memory.h"
#include "absl/synchronization/mutex.h"
#include "stratum/glue/gtl/map_util.h"
#include "stratum/glue/integral_types.h"
#include "stratum/glue/logging.h"
#include "stratum/glue/status/status_macros.h"
//#include "bfIntf/bf_chassis_manager.h"
#include "stratum/hal/lib/tdi/tdi_chassis_manager.h"
#include "stratum/hal/lib/tdi/tdi_node.h"
#include "stratum/hal/lib/tdi/utils.h"
#include "stratum/lib/constants.h"
#include "stratum/lib/macros.h"

namespace stratum {
namespace hal {
namespace barefoot {

TdiSwitch::TdiSwitch(PhalInterface* phal_interface,
                     TdiChassisManager* tdi_chassis_manager,
                     TdiSdeInterface* tdi_sde_interface,
                     const std::map<int, TdiNode*>& device_id_to_tdi_node)
    : phal_interface_(ABSL_DIE_IF_NULL(phal_interface)),
      tdi_sde_interface_(ABSL_DIE_IF_NULL(tdi_sde_interface)),
      tdi_chassis_manager_(ABSL_DIE_IF_NULL(tdi_chassis_manager)),
      device_id_to_tdi_node_(device_id_to_tdi_node),
      node_id_to_tdi_node_() {
  for (const auto& entry : device_id_to_tdi_node_) {
    CHECK_GE(entry.first, 0)
        << "Invalid device_id number " << entry.first << ".";
/*
    CHECK_NE(entry.second, nullptr)
        << "Detected null TdiNode for device_id " << entry.first << ".";
*/

  }
}

TdiSwitch::~TdiSwitch() {}

::util::Status TdiSwitch::PushChassisConfig(const ChassisConfig& config) {
  absl::WriterMutexLock l(&chassis_lock);
  RETURN_IF_ERROR(phal_interface_->PushChassisConfig(config));
  RETURN_IF_ERROR(tdi_chassis_manager_->PushChassisConfig(config));
  ASSIGN_OR_RETURN(const auto& node_id_to_device_id,
                   tdi_chassis_manager_->GetNodeIdToUnitMap());
  node_id_to_tdi_node_.clear();
  for (const auto& entry : node_id_to_device_id) {
    uint64 node_id = entry.first;
    int device_id = entry.second;
    ASSIGN_OR_RETURN(auto* tdi_node, GetTdiNodeFromDeviceId(device_id));
    RETURN_IF_ERROR(tdi_node->PushChassisConfig(config, node_id));
    node_id_to_tdi_node_[node_id] = tdi_node;
  }

  LOG(INFO) << "Chassis config pushed successfully.";

  return ::util::OkStatus();
}

::util::Status TdiSwitch::VerifyChassisConfig(const ChassisConfig& config) {
  (void)config;
  return ::util::OkStatus();
}

::util::Status TdiSwitch::PushForwardingPipelineConfig(
    uint64 node_id, const ::p4::v1::ForwardingPipelineConfig& config) {
  absl::WriterMutexLock l(&chassis_lock);
  ASSIGN_OR_RETURN(auto* tdi_node, GetTdiNodeFromNodeId(node_id));
  RETURN_IF_ERROR(tdi_node->PushForwardingPipelineConfig(config));
  RETURN_IF_ERROR(tdi_chassis_manager_->ReplayPortsConfig(node_id));

  LOG(INFO) << "P4-based forwarding pipeline config pushed successfully to "
            << "node with ID " << node_id << ".";

  ASSIGN_OR_RETURN(const auto& node_id_to_device_id,
                   tdi_chassis_manager_->GetNodeIdToUnitMap());

  RET_CHECK(gtl::ContainsKey(node_id_to_device_id, node_id))
      << "Unable to find device_id number for node " << node_id;
  int device_id = gtl::FindOrDie(node_id_to_device_id, node_id);
  ASSIGN_OR_RETURN(auto cpu_port, tdi_sde_interface_->GetPcieCpuPort(device_id));
  RETURN_IF_ERROR(tdi_sde_interface_->SetTmCpuPort(device_id, cpu_port));
  return ::util::OkStatus();
}

::util::Status TdiSwitch::SaveForwardingPipelineConfig(
    uint64 node_id, const ::p4::v1::ForwardingPipelineConfig& config) {
  absl::WriterMutexLock l(&chassis_lock);
  ASSIGN_OR_RETURN(auto* tdi_node, GetTdiNodeFromNodeId(node_id));
  RETURN_IF_ERROR(tdi_node->SaveForwardingPipelineConfig(config));
  RETURN_IF_ERROR(tdi_chassis_manager_->ReplayPortsConfig(node_id));

  LOG(INFO) << "P4-based forwarding pipeline config saved successfully to "
            << "node with ID " << node_id << ".";

  return ::util::OkStatus();
}

::util::Status TdiSwitch::CommitForwardingPipelineConfig(uint64 node_id) {
  absl::WriterMutexLock l(&chassis_lock);
  ASSIGN_OR_RETURN(auto* tdi_node, GetTdiNodeFromNodeId(node_id));
  RETURN_IF_ERROR(tdi_node->CommitForwardingPipelineConfig());

  LOG(INFO) << "P4-based forwarding pipeline config committed successfully to "
            << "node with ID " << node_id << ".";

  return ::util::OkStatus();
}

::util::Status TdiSwitch::VerifyForwardingPipelineConfig(
    uint64 node_id, const ::p4::v1::ForwardingPipelineConfig& config) {
  absl::WriterMutexLock l(&chassis_lock);
  ASSIGN_OR_RETURN(auto* tdi_node, GetTdiNodeFromNodeId(node_id));
  return tdi_node->VerifyForwardingPipelineConfig(config);
}

::util::Status TdiSwitch::Shutdown() {
  ::util::Status status = ::util::OkStatus();
  for (const auto& entry : device_id_to_tdi_node_) {
    TdiNode* node = entry.second;
    APPEND_STATUS_IF_ERROR(status, node->Shutdown());
  }
  APPEND_STATUS_IF_ERROR(status, tdi_chassis_manager_->Shutdown());
  APPEND_STATUS_IF_ERROR(status, phal_interface_->Shutdown());
  // APPEND_STATUS_IF_ERROR(status, tdi_sde_interface_->Shutdown());

  return status;
}

::util::Status TdiSwitch::Freeze() { return ::util::OkStatus(); }

::util::Status TdiSwitch::Unfreeze() { return ::util::OkStatus(); }

::util::Status TdiSwitch::WriteForwardingEntries(
    const ::p4::v1::WriteRequest& req, std::vector<::util::Status>* results) {
  if (!req.updates_size()) return ::util::OkStatus();  // nothing to do.
  RET_CHECK(req.device_id()) << "No device_id in WriteRequest.";
  RET_CHECK(results != nullptr)
      << "Need to provide non-null results pointer for non-empty updates.";

  absl::ReaderMutexLock l(&chassis_lock);
  ASSIGN_OR_RETURN(auto* tdi_node, GetTdiNodeFromNodeId(req.device_id()));
  return tdi_node->WriteForwardingEntries(req, results);
}

::util::Status TdiSwitch::ReadForwardingEntries(
    const ::p4::v1::ReadRequest& req,
    WriterInterface<::p4::v1::ReadResponse>* writer,
    std::vector<::util::Status>* details) {
  RET_CHECK(req.device_id()) << "No device_id in ReadRequest.";
  RET_CHECK(writer) << "Channel writer must be non-null.";
  RET_CHECK(details) << "Details pointer must be non-null.";

  absl::ReaderMutexLock l(&chassis_lock);
  ASSIGN_OR_RETURN(auto* tdi_node, GetTdiNodeFromNodeId(req.device_id()));
  return tdi_node->ReadForwardingEntries(req, writer, details);
}

::util::Status TdiSwitch::RegisterStreamMessageResponseWriter(
    uint64 node_id,
    std::shared_ptr<WriterInterface<::p4::v1::StreamMessageResponse>> writer) {
  ASSIGN_OR_RETURN(auto* tdi_node, GetTdiNodeFromNodeId(node_id));
  return tdi_node->RegisterStreamMessageResponseWriter(writer);
}

::util::Status TdiSwitch::UnregisterStreamMessageResponseWriter(
    uint64 node_id) {
  ASSIGN_OR_RETURN(auto* tdi_node, GetTdiNodeFromNodeId(node_id));
  return tdi_node->UnregisterStreamMessageResponseWriter();
}

::util::Status TdiSwitch::HandleStreamMessageRequest(
    uint64 node_id, const ::p4::v1::StreamMessageRequest& request) {
  ASSIGN_OR_RETURN(auto* tdi_node, GetTdiNodeFromNodeId(node_id));
  return tdi_node->HandleStreamMessageRequest(request);
}

::util::Status TdiSwitch::RegisterEventNotifyWriter(
    std::shared_ptr<WriterInterface<GnmiEventPtr>> writer) {
  return tdi_chassis_manager_->RegisterEventNotifyWriter(writer);
}

::util::Status TdiSwitch::UnregisterEventNotifyWriter() {
  return tdi_chassis_manager_->UnregisterEventNotifyWriter();
}

::util::Status TdiSwitch::RetrieveValue(uint64 node_id,
                                         const DataRequest& request,
                                         WriterInterface<DataResponse>* writer,
                                         std::vector<::util::Status>* details) {
  absl::ReaderMutexLock l(&chassis_lock);
  for (const auto& req : request.requests()) {
    DataResponse resp;
    ::util::Status status = ::util::OkStatus();
    switch (req.request_case()) {
      case DataRequest::Request::kOperStatus:
      case DataRequest::Request::kAdminStatus:
      case DataRequest::Request::kMacAddress:
      case DataRequest::Request::kPortSpeed:
      case DataRequest::Request::kNegotiatedPortSpeed:
      case DataRequest::Request::kLacpRouterMac:
      case DataRequest::Request::kPortCounters:
      case DataRequest::Request::kForwardingViability:
      case DataRequest::Request::kHealthIndicator:
      case DataRequest::Request::kAutonegStatus:
      case DataRequest::Request::kFrontPanelPortInfo:
      case DataRequest::Request::kLoopbackStatus:
      case DataRequest::Request::kSdnPortId: {
        auto port_data = tdi_chassis_manager_->GetPortData(req);
        if (!port_data.ok()) {
          status.Update(port_data.status());
        } else {
          resp = port_data.ConsumeValueOrDie();
        }
        break;
      }
      case DataRequest::Request::kNodeInfo: {
        auto device_id =
            tdi_chassis_manager_->GetUnitFromNodeId(req.node_info().node_id());
        if (!device_id.ok()) {
          status.Update(device_id.status());
        } else {
          auto* node_info = resp.mutable_node_info();
          node_info->set_vendor_name("Barefoot");
          node_info->set_chip_name(
              tdi_sde_interface_->GetBfChipType(device_id.ValueOrDie()));
        }
        break;
      }
      default:
        status =
            MAKE_ERROR(ERR_UNIMPLEMENTED)
            << "DataRequest field "
            << req.descriptor()->FindFieldByNumber(req.request_case())->name()
            << " is not supported yet!";
        break;
    }
    if (status.ok()) {
      // If everything is OK send it to the caller.
      writer->Write(resp);
    }
    if (details) details->push_back(status);
  }
  return ::util::OkStatus();
}

::util::Status TdiSwitch::SetValue(uint64 node_id, const SetRequest& request,
                                    std::vector<::util::Status>* details) {
  LOG(INFO) << "TdiSwitch::SetValue is not implemented yet, but changes will "
            << "be performed when ChassisConfig is pushed again. "
            << request.ShortDebugString() << ".";

  return ::util::OkStatus();
}

::util::StatusOr<std::vector<std::string>> TdiSwitch::VerifyState() {
  return std::vector<std::string>();
}

std::unique_ptr<TdiSwitch> TdiSwitch::CreateInstance(
    PhalInterface* phal_interface, TdiChassisManager* tdi_chassis_manager,
    TdiSdeInterface* tdi_sde_interface,
    const std::map<int, TdiNode*>& device_id_to_tdi_node) {
  return absl::WrapUnique(new TdiSwitch(phal_interface, tdi_chassis_manager,
                                        tdi_sde_interface,
                                        device_id_to_tdi_node));
}

::util::StatusOr<TdiNode*> TdiSwitch::GetTdiNodeFromDeviceId(
    int device_id) const {
  TdiNode* tdi_node = gtl::FindPtrOrNull(device_id_to_tdi_node_, device_id);
  if (tdi_node == nullptr) {
    return MAKE_ERROR(ERR_INVALID_PARAM)
           << "Unit " << device_id << " is unknown.";
  }
  return tdi_node;
}

::util::StatusOr<TdiNode*> TdiSwitch::GetTdiNodeFromNodeId(
    uint64 node_id) const {
  TdiNode* tdi_node = gtl::FindPtrOrNull(node_id_to_tdi_node_, node_id);
  if (tdi_node == nullptr) {
    return MAKE_ERROR(ERR_INVALID_PARAM)
           << "Node with ID " << node_id
           << " is unknown or no config has been pushed to it yet.";
  }
  return tdi_node;
}

}  // namespace barefoot
}  // namespace hal
}  // namespace stratum
