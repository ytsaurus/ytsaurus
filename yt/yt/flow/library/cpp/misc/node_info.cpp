#include "node_info.h"

#include <yt/yt/flow/library/cpp/misc/proto/node_info.pb.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

std::string TNodeInfoBase::GetIdentifyingString() const
{
    return Format("{name=%Qv; rpc_address=%Qv; incarnation_id=%Qv}",
        Name,
        RpcAddress,
        IncarnationId);
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TNodeInfo* protoInfo, const TNodeInfoBase& info)
{
    protoInfo->set_name(info.Name);
    protoInfo->set_rpc_address(info.RpcAddress);
    protoInfo->set_monitoring_address(info.MonitoringAddress);
    protoInfo->set_remote_shell_command(info.RemoteShellCommand);
    ToProto(protoInfo->mutable_incarnation_id(), info.IncarnationId);
    if (info.VcpuFactor) {
        protoInfo->set_vcpu_factor(info.VcpuFactor.value());
    } else {
        protoInfo->clear_vcpu_factor();
    }
    protoInfo->set_build_version(info.BuildVersion);
    if (info.VcpuLimit) {
        protoInfo->set_vcpu_limit(info.VcpuLimit.value());
    } else {
        protoInfo->clear_vcpu_limit();
    }
    protoInfo->set_flow_core_version(info.FlowCoreVersion);
    protoInfo->set_build_type(info.BuildType);
}

void FromProto(TNodeInfoBase* info, const NProto::TNodeInfo& protoInfo)
{
    info->Name = protoInfo.name();
    info->RpcAddress = protoInfo.rpc_address();
    info->MonitoringAddress = protoInfo.monitoring_address();
    info->RemoteShellCommand = protoInfo.remote_shell_command();
    info->IncarnationId = ::NYT::FromProto<TIncarnationId>(protoInfo.incarnation_id());
    if (protoInfo.has_vcpu_factor()) {
        info->VcpuFactor = protoInfo.vcpu_factor();
    } else {
        info->VcpuFactor = std::nullopt;
    }
    info->BuildVersion = protoInfo.build_version();
    if (protoInfo.has_vcpu_limit()) {
        info->VcpuLimit = protoInfo.vcpu_limit();
    } else {
        info->VcpuLimit = std::nullopt;
    }
    info->FlowCoreVersion = protoInfo.flow_core_version();
    info->BuildType = protoInfo.build_type();
}

////////////////////////////////////////////////////////////////////////////////

void TNodeInfo::Register(TRegistrar registrar)
{
    RegisterNodeInfoStruct(registrar);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
