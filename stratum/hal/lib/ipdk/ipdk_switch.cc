// Copyright 2020-present Open Networking Foundation
// Copyright 2022 Intel Corporation
// SPDX-License-Identifier: Apache-2.0

#include "stratum/hal/lib/ipdk/ipdk_switch.h"

#include <algorithm>
#include <map>
#include <vector>

#include "absl/memory/memory.h"
#include "absl/synchronization/mutex.h"
#include "stratum/glue/gtl/map_util.h"
#include "stratum/glue/integral_types.h"
#include "stratum/glue/logging.h"
#include "stratum/glue/status/status_macros.h"
#include "stratum/hal/lib/ipdk/ipdk_chassis_manager.h"
#include "stratum/hal/lib/ipdk/ipdk_node.h"
#include "stratum/hal/lib/tdi/utils.h"
#include "stratum/lib/constants.h"
#include "stratum/lib/macros.h"

namespace stratum {
namespace hal {
namespace barefoot {

IpdkSwitch::IpdkSwitch(PhalInterface* phal_interface,
                       IpdkChassisManager* chassis_manager,
                       TdiSdeInterface* sde_interface,
                       const std::map<int, IpdkNode*>& device_id_to_ipdk_node)
    : phal_interface_(ABSL_DIE_IF_NULL(phal_interface)),
      sde_interface_(ABSL_DIE_IF_NULL(sde_interface)),
      chassis_manager_(ABSL_DIE_IF_NULL(chassis_manager)),
      device_id_to_ipdk_node_(device_id_to_ipdk_node),
      node_id_to_ipdk_node_() {
  for (const auto& entry : device_id_to_ipdk_node_) {
    CHECK_GE(entry.first, 0)
        << "Invalid device_id number " << entry.first << ".";
#ifndef P4OVS_CHANGES
    // P4OVS_CHANGES: Why suppress the null pointer check?
    CHECK_NE(entry.second, nullptr)
        << "Detected null IpdkNode for device_id " << entry.first << ".";
#endif
  }
}

IpdkSwitch::~IpdkSwitch() {}

::util::Status IpdkSwitch::PushChassisConfig(const ChassisConfig& config) {
  absl::WriterMutexLock l(&chassis_lock);
  RETURN_IF_ERROR(phal_interface_->PushChassisConfig(config));
  RETURN_IF_ERROR(chassis_manager_->PushChassisConfig(config));
  ASSIGN_OR_RETURN(const auto& node_id_to_device_id,
                   chassis_manager_->GetNodeIdToUnitMap());
  node_id_to_ipdk_node_.clear();
  for (const auto& entry : node_id_to_device_id) {
    uint64 node_id = entry.first;
    int device_id = entry.second;
    ASSIGN_OR_RETURN(auto* ipdk_node, GetIpdkNodeFromDeviceId(device_id));
    RETURN_IF_ERROR(ipdk_node->PushChassisConfig(config, node_id));
    node_id_to_ipdk_node_[node_id] = ipdk_node;
  }

  LOG(INFO) << "Chassis config pushed successfully.";

  return ::util::OkStatus();
}

::util::Status IpdkSwitch::VerifyChassisConfig(const ChassisConfig& config) {
  (void)config;
  return ::util::OkStatus();
}

::util::Status IpdkSwitch::PushForwardingPipelineConfig(
    uint64 node_id, const ::p4::v1::ForwardingPipelineConfig& config) {
  absl::WriterMutexLock l(&chassis_lock);
  ASSIGN_OR_RETURN(auto* ipdk_node, GetIpdkNodeFromNodeId(node_id));
  RETURN_IF_ERROR(ipdk_node->PushForwardingPipelineConfig(config));
#ifndef P4OVS_CHANGES
  // P4OVS_CHANGES:
  // - Reason for change?
  // - Contexts in which it applies? E.g., does it apply to Tofino?
  // - Temporary or permanent?
  // - If temporary, what are the prerequisites for reverting it?
  //
  RETURN_IF_ERROR(chassis_manager_->ReplayPortsConfig(node_id));

  LOG(INFO) << "P4-based forwarding pipeline config pushed successfully to "
            << "node with ID " << node_id << ".";

  ASSIGN_OR_RETURN(const auto& node_id_to_device_id,
                   chassis_manager_->GetNodeIdToUnitMap());

  CHECK_RETURN_IF_FALSE(gtl::ContainsKey(node_id_to_device_id, node_id))
      << "Unable to find device_id number for node " << node_id;
  int device_id = gtl::FindOrDie(node_id_to_device_id, node_id);
  ASSIGN_OR_RETURN(auto cpu_port, sde_interface_->GetPcieCpuPort(device_id));
  RETURN_IF_ERROR(sde_interface_->SetTmCpuPort(device_id, cpu_port));
#endif
  return ::util::OkStatus();
}

::util::Status IpdkSwitch::SaveForwardingPipelineConfig(
    uint64 node_id, const ::p4::v1::ForwardingPipelineConfig& config) {
  absl::WriterMutexLock l(&chassis_lock);
  ASSIGN_OR_RETURN(auto* ipdk_node, GetIpdkNodeFromNodeId(node_id));
  RETURN_IF_ERROR(ipdk_node->SaveForwardingPipelineConfig(config));
#ifndef P4OVS_CHANGES
  // P4OVS_CHANGES:
  // - Reason for change?
  // - Contexts in which it applies? E.g., does it apply to Tofino?
  // - Temporary or permanent?
  // - If temporary, what are the prerequisites for reverting it?
  //
  RETURN_IF_ERROR(chassis_manager_->ReplayPortsConfig(node_id));

  LOG(INFO) << "P4-based forwarding pipeline config saved successfully to "
            << "node with ID " << node_id << ".";
#endif
  return ::util::OkStatus();
}

::util::Status IpdkSwitch::CommitForwardingPipelineConfig(uint64 node_id) {
  absl::WriterMutexLock l(&chassis_lock);
  ASSIGN_OR_RETURN(auto* ipdk_node, GetIpdkNodeFromNodeId(node_id));
  RETURN_IF_ERROR(ipdk_node->CommitForwardingPipelineConfig());
#ifndef P4OVS_CHANGES
  // P4OVS_CHANGES: Why suppress this log message?
  LOG(INFO) << "P4-based forwarding pipeline config committed successfully to "
            << "node with ID " << node_id << ".";
#endif
  return ::util::OkStatus();
}

::util::Status IpdkSwitch::VerifyForwardingPipelineConfig(
    uint64 node_id, const ::p4::v1::ForwardingPipelineConfig& config) {
  absl::WriterMutexLock l(&chassis_lock);
  ASSIGN_OR_RETURN(auto* ipdk_node, GetIpdkNodeFromNodeId(node_id));
  return ipdk_node->VerifyForwardingPipelineConfig(config);
}

::util::Status IpdkSwitch::Shutdown() {
  ::util::Status status = ::util::OkStatus();
  for (const auto& entry : device_id_to_ipdk_node_) {
    IpdkNode* node = entry.second;
    APPEND_STATUS_IF_ERROR(status, node->Shutdown());
  }
  APPEND_STATUS_IF_ERROR(status, chassis_manager_->Shutdown());
  APPEND_STATUS_IF_ERROR(status, phal_interface_->Shutdown());
  // APPEND_STATUS_IF_ERROR(status, sde_interface_->Shutdown());

  return status;
}

::util::Status IpdkSwitch::Freeze() { return ::util::OkStatus(); }

::util::Status IpdkSwitch::Unfreeze() { return ::util::OkStatus(); }

::util::Status IpdkSwitch::WriteForwardingEntries(
    const ::p4::v1::WriteRequest& req, std::vector<::util::Status>* results) {
#ifdef P4OVS_CHANGES
  if (!req.updates_size()) return ::util::OkStatus();  // nothing to do.
  CHECK_RETURN_IF_FALSE(req.device_id()) << "No device_id in WriteRequest.";
  CHECK_RETURN_IF_FALSE(results != nullptr) << "Results pointer must be non-null.";
#endif
  absl::ReaderMutexLock l(&chassis_lock);
  ASSIGN_OR_RETURN(auto* ipdk_node, GetIpdkNodeFromNodeId(req.device_id()));
  return ipdk_node->WriteForwardingEntries(req, results);
}

::util::Status IpdkSwitch::ReadForwardingEntries(
    const ::p4::v1::ReadRequest& req,
    WriterInterface<::p4::v1::ReadResponse>* writer,
    std::vector<::util::Status>* details) {
#ifdef P4OVS_CHANGES
  CHECK_RETURN_IF_FALSE(req.device_id()) << "No device_id in ReadRequest.";
  CHECK_RETURN_IF_FALSE(writer) << "Channel writer must be non-null.";
  CHECK_RETURN_IF_FALSE(details) << "Details pointer must be non-null.";
#endif
  absl::ReaderMutexLock l(&chassis_lock);
  ASSIGN_OR_RETURN(auto* ipdk_node, GetIpdkNodeFromNodeId(req.device_id()));
  return ipdk_node->ReadForwardingEntries(req, writer, details);
}

::util::Status IpdkSwitch::RegisterStreamMessageResponseWriter(
    uint64 node_id,
    std::shared_ptr<WriterInterface<::p4::v1::StreamMessageResponse>> writer) {
  ASSIGN_OR_RETURN(auto* ipdk_node, GetIpdkNodeFromNodeId(node_id));
  return ipdk_node->RegisterStreamMessageResponseWriter(writer);
}

::util::Status IpdkSwitch::UnregisterStreamMessageResponseWriter(
    uint64 node_id) {
  ASSIGN_OR_RETURN(auto* ipdk_node, GetIpdkNodeFromNodeId(node_id));
  return ipdk_node->UnregisterStreamMessageResponseWriter();
}

::util::Status IpdkSwitch::HandleStreamMessageRequest(
    uint64 node_id, const ::p4::v1::StreamMessageRequest& request) {
  ASSIGN_OR_RETURN(auto* ipdk_node, GetIpdkNodeFromNodeId(node_id));
  return ipdk_node->HandleStreamMessageRequest(request);
}

::util::Status IpdkSwitch::RegisterEventNotifyWriter(
    std::shared_ptr<WriterInterface<GnmiEventPtr>> writer) {
  return chassis_manager_->RegisterEventNotifyWriter(writer);
}

::util::Status IpdkSwitch::UnregisterEventNotifyWriter() {
  return chassis_manager_->UnregisterEventNotifyWriter();
}

::util::Status IpdkSwitch::RetrieveValue(
    uint64 node_id, const DataRequest& request,
    WriterInterface<DataResponse>* writer,
    std::vector<::util::Status>* details) {
  absl::ReaderMutexLock l(&chassis_lock);
  for (const auto& req : request.requests()) {
    DataResponse resp;
    ::util::Status status = ::util::OkStatus();
    switch (req.request_case()) {
      //
      // IPDK CHANGES: Why do we screen for supported Request types here?
      //
      // It seems to me that we could:
      // 1) Check for types that are processed by the Switch class (such as
      //    kNodeInfo) and pass everything else through to ChassisManager.
      // 2) Modify ChassisManager to return ERR_UNIMPLEMENTED (as is
      //    currently done in the Switch class) instead of ERR_INTERNAL.
      //
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
        auto port_data = chassis_manager_->GetPortData(req);
        if (!port_data.ok()) {
          status.Update(port_data.status());
        } else {
          resp = port_data.ConsumeValueOrDie();
        }
        break;
      }
      case DataRequest::Request::kNodeInfo: {
        auto device_id =
            chassis_manager_->GetUnitFromNodeId(req.node_info().node_id());
        if (!device_id.ok()) {
          status.Update(device_id.status());
        } else {
          auto* node_info = resp.mutable_node_info();
          //
          // IPDK CHANGES:
	  // - Do we want to return IPDK as the vendor name, or should we
	  //   call another source (the SDE or a personality module) to obtain
	  //   a more specific or customizable value?
	  //
          // - The same thing applies to getting the Chip Type. It makes sense
	  //   to call GetBfChipType() when you know you're using the Barefoot
	  //   SDE. The generic solution would be to call a GetChipName()
	  //   method in the SDE or a personality module.
          //
          node_info->set_vendor_name("IPDK");
          node_info->set_chip_name(
              sde_interface_->GetBfChipType(device_id.ValueOrDie()));
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

::util::Status IpdkSwitch::SetValue(uint64 node_id, const SetRequest& request,
                                    std::vector<::util::Status>* details) {
  LOG(INFO) << "IpdkSwitch::SetValue is not implemented yet. Changes will "
            << "be applied when ChassisConfig is pushed again. "
            << request.ShortDebugString() << ".";

  return ::util::OkStatus();
}

::util::StatusOr<std::vector<std::string>> IpdkSwitch::VerifyState() {
  return std::vector<std::string>();
}

bool IpdkSwitch::PortParamAlreadySet(
    uint64 node_id, uint32 port_id,
    SetRequest::Request::Port::ValueCase value_case) {
  return chassis_manager_->PortParamAlreadySet(node_id, port_id, value_case);
}

::util::Status IpdkSwitch::SetPortParam(
    uint64 node_id, uint32 port_id,
    const SingletonPort& singleton_port,
    SetRequest::Request::Port::ValueCase value_case) {
  return chassis_manager_->SetPortParam(node_id, port_id, singleton_port,
                                        value_case);
}

std::unique_ptr<IpdkSwitch> IpdkSwitch::CreateInstance(
    PhalInterface* phal_interface, IpdkChassisManager* chassis_manager,
    TdiSdeInterface* sde_interface,
    const std::map<int, IpdkNode*>& device_id_to_ipdk_node) {
  return absl::WrapUnique(
         new IpdkSwitch(phal_interface, chassis_manager, sde_interface,
                        device_id_to_ipdk_node));
}

::util::StatusOr<IpdkNode*> IpdkSwitch::GetIpdkNodeFromDeviceId(
    int device_id) const {
  IpdkNode* ipdk_node = gtl::FindPtrOrNull(device_id_to_ipdk_node_, device_id);
  if (ipdk_node == nullptr) {
    return MAKE_ERROR(ERR_INVALID_PARAM)
           << "Unit " << device_id << " is unknown.";
  }
  return ipdk_node;
}

::util::StatusOr<IpdkNode*> IpdkSwitch::GetIpdkNodeFromNodeId(
    uint64 node_id) const {
  IpdkNode* ipdk_node = gtl::FindPtrOrNull(node_id_to_ipdk_node_, node_id);
  if (ipdk_node == nullptr) {
    return MAKE_ERROR(ERR_INVALID_PARAM)
           << "Node with ID " << node_id
           << " is unknown or no config has been pushed to it yet.";
  }
  return ipdk_node;
}

}  // namespace barefoot
}  // namespace hal
}  // namespace stratum
