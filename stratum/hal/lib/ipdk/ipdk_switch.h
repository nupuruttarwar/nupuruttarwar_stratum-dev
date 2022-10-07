// Copyright 2020-present Open Networking Foundation
// Copyright 2022 Intel Corporation
// SPDX-License-Identifier: Apache-2.0

#ifndef STRATUM_HAL_LIB_IPDK_IPDK_SWITCH_H_
#define STRATUM_HAL_LIB_IPDK_IPDK_SWITCH_H_

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "stratum/hal/lib/common/phal_interface.h"
#include "stratum/hal/lib/common/switch_interface.h"
#include "stratum/hal/lib/ipdk/ipdk_chassis_manager.h"

namespace stratum {
namespace hal {
namespace barefoot {

class TdiSdeInterface;
class IpdkNode;

class PortParamInterface {
public:
    virtual ~PortParamInterface() {}

    virtual bool PortParamAlreadySet(
        uint64 node_id, uint32 port_id,
        SetRequest::Request::Port::ValueCase value_case) = 0;

    virtual ::util::Status SetPortParam(
        uint64 node_id, uint32 port_id,
        const SingletonPort& singleton_port,
        SetRequest::Request::Port::ValueCase value_case) = 0;
};

class IpdkSwitch : virtual public SwitchInterface,
                   virtual public PortParamInterface {
 public:
  ~IpdkSwitch() override;

  // SwitchInterface public methods.
  ::util::Status PushChassisConfig(const ChassisConfig& config) override
      LOCKS_EXCLUDED(chassis_lock);
  ::util::Status VerifyChassisConfig(const ChassisConfig& config) override
      LOCKS_EXCLUDED(chassis_lock);
  ::util::Status PushForwardingPipelineConfig(
      uint64 node_id, const ::p4::v1::ForwardingPipelineConfig& config) override
      LOCKS_EXCLUDED(chassis_lock);
  ::util::Status SaveForwardingPipelineConfig(
      uint64 node_id, const ::p4::v1::ForwardingPipelineConfig& config) override
      LOCKS_EXCLUDED(chassis_lock);
  ::util::Status CommitForwardingPipelineConfig(uint64 node_id) override
      LOCKS_EXCLUDED(chassis_lock);
  ::util::Status VerifyForwardingPipelineConfig(
      uint64 node_id, const ::p4::v1::ForwardingPipelineConfig& config) override
      LOCKS_EXCLUDED(chassis_lock);
  ::util::Status Shutdown() override LOCKS_EXCLUDED(chassis_lock);
  ::util::Status Freeze() override;
  ::util::Status Unfreeze() override;
  ::util::Status WriteForwardingEntries(const ::p4::v1::WriteRequest& req,
                                        std::vector<::util::Status>* results)
      override LOCKS_EXCLUDED(chassis_lock);
  ::util::Status ReadForwardingEntries(
      const ::p4::v1::ReadRequest& req,
      WriterInterface<::p4::v1::ReadResponse>* writer,
      std::vector<::util::Status>* details) override
      LOCKS_EXCLUDED(chassis_lock);
  ::util::Status RegisterStreamMessageResponseWriter(
      uint64 node_id,
      std::shared_ptr<WriterInterface<::p4::v1::StreamMessageResponse>> writer)
      override LOCKS_EXCLUDED(chassis_lock);
  ::util::Status UnregisterStreamMessageResponseWriter(uint64 node_id) override
      LOCKS_EXCLUDED(chassis_lock);
  ::util::Status HandleStreamMessageRequest(
      uint64 node_id, const ::p4::v1::StreamMessageRequest& request) override
      LOCKS_EXCLUDED(chassis_lock);
  ::util::Status RegisterEventNotifyWriter(
      std::shared_ptr<WriterInterface<GnmiEventPtr>> writer) override
      LOCKS_EXCLUDED(chassis_lock);
  ::util::Status UnregisterEventNotifyWriter() override
      LOCKS_EXCLUDED(chassis_lock) LOCKS_EXCLUDED(chassis_lock);
  ::util::Status RetrieveValue(uint64 node_id, const DataRequest& requests,
                               WriterInterface<DataResponse>* writer,
                               std::vector<::util::Status>* details) override
      LOCKS_EXCLUDED(chassis_lock);
  ::util::Status SetValue(uint64 node_id, const SetRequest& request,
                          std::vector<::util::Status>* details) override
      LOCKS_EXCLUDED(chassis_lock);
  ::util::StatusOr<std::vector<std::string>> VerifyState() override;

  // Determines whether the specified port configuration parameter has
  // already been set. Once set, it may not be set again.
  bool PortParamAlreadySet(
      uint64 node_id, uint32 port_id,
      SetRequest::Request::Port::ValueCase value_case) override;

  // Sets the value of a port configuration parameter.
  // Once set, it may not be set again.
  ::util::Status SetPortParam(
      uint64 node_id, uint32 port_id,
      const SingletonPort& singleton_port,
      SetRequest::Request::Port::ValueCase value_case) override;

  // Factory function for creating the instance of the class.
  static std::unique_ptr<IpdkSwitch> CreateInstance(
      PhalInterface* phal_interface,
      IpdkChassisManager* chassis_manager,
      TdiSdeInterface* sde_interface,
      const std::map<int, IpdkNode*>& device_id_to_ipdk_node);

  // IpdkSwitch is neither copyable nor movable.
  IpdkSwitch(const IpdkSwitch&) = delete;
  IpdkSwitch& operator=(const IpdkSwitch&) = delete;
  IpdkSwitch(IpdkSwitch&&) = delete;
  IpdkSwitch& operator=(IpdkSwitch&&) = delete;

 private:
  // Private constructor. Use CreateInstance() to create an instance of this
  // class.
  IpdkSwitch(PhalInterface* phal_interface,
             IpdkChassisManager* chassis_manager,
             TdiSdeInterface* sde_interface,
             const std::map<int, IpdkNode*>& device_id_to_ipdk_node);

  // Helper to get IpdkNode pointer from device_id number or return error
  // indicating invalid device_id.
  ::util::StatusOr<IpdkNode*> GetIpdkNodeFromDeviceId(int device_id) const;

  // Helper to get IpdkNode pointer from node id or return error indicating
  // invalid/unknown/uninitialized node.
  ::util::StatusOr<IpdkNode*> GetIpdkNodeFromNodeId(uint64 node_id) const;

  // Pointer to a PhalInterface implementation. The pointer has been also
  // passed to a few managers for accessing HW. Note that there is only one
  // instance of this class per chassis.
  PhalInterface* phal_interface_;  // not owned by this class.

  // Pointer to a TdiSdeInterface implementation that wraps TDI API calls.
  TdiSdeInterface* sde_interface_;  // not owned by this class.

  // Per chassis Managers. Note that there is only one instance of this class
  // per chassis.
  IpdkChassisManager* chassis_manager_;  // not owned by the class.

  // Map from zero-based device_id number corresponding to a node/ASIC to a
  // pointer to IpdkNode which contains all the per-node managers for that
  // node/ASIC. This map is initialized in the constructor and will not change
  // during the lifetime of the class.
  // TODO(max): Does this need to be protected by chassis_lock?
  const std::map<int, IpdkNode*> device_id_to_ipdk_node_;  // pointers not owned

  // Map from the node ids to to a pointer to IpdkNode which contain all the
  // per-node managers for that node/ASIC. Created whenever a config is pushed.
  // At any point in time, this map will contain as keys the ids of the nodes
  // that had a successful config push.
  // TODO(max): Does this need to be protected by chassis_lock?
  std::map<uint64, IpdkNode*> node_id_to_ipdk_node_;  //  pointers not owned
};

}  // namespace barefoot
}  // namespace hal
}  // namespace stratum

#endif  // STRATUM_HAL_LIB_IPDK_IPDK_SWITCH_H_
