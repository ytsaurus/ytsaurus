#pragma once

#include <yt/yt/flow/library/cpp/common/public.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TNodeInfo;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

struct TNodeInfoBase
{
    //! FQDN or something more human-readable than IP.
    std::string Name;

    //! Address of RPC server of node.
    std::string RpcAddress;

    //! Address of monitoring HTTP server of node.
    std::string MonitoringAddress;

    //! Remote shell command (ssh or other type of remote shell).
    std::string RemoteShellCommand;

    //! Guid of node process.
    TIncarnationId IncarnationId;

    //! VCPU factor on node. CpuLimit * VcpuFactor = VcpuLimit where CpuLimit is physical CPU limit in milli-cores (1000 = 1 core).
    std::optional<double> VcpuFactor;
    //! Virtual CPU limit on node, in milli-vcores.
    std::optional<double> VcpuLimit;

    std::string BuildVersion;

    std::string BinaryChecksum;

    //! Flow-core binary version.
    std::string FlowCoreVersion;

    //! `ya make` build type (e.g. "release", "debug") or the active sanitizer name
    //! ("ASAN"/"TSAN"/"MSAN"). Empty for nodes from binaries that predate this field.
    std::string BuildType;

    // Name + RpcAddress + IncarnationId.
    std::string GetIdentifyingString() const;
};

void ToProto(NProto::TNodeInfo* protoInfo, const TNodeInfoBase& info);
void FromProto(TNodeInfoBase* info, const NProto::TNodeInfo& protoInfo);

////////////////////////////////////////////////////////////////////////////////

struct TNodeInfo
    : public NYTree::TYsonStruct
    , public TNodeInfoBase
{
    REGISTER_YSON_STRUCT(TNodeInfo);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TNodeInfo);

////////////////////////////////////////////////////////////////////////////////

template <typename TRegistrar>
void RegisterNodeInfoStruct(TRegistrar registrar)
{
    registrar.BaseClassParameter("name", &TNodeInfoBase::Name)
        .Default();
    registrar.BaseClassParameter("rpc_address", &TNodeInfoBase::RpcAddress)
        .Default();
    registrar.BaseClassParameter("monitoring_address", &TNodeInfoBase::MonitoringAddress)
        .Default();
    registrar.BaseClassParameter("remote_shell_command", &TNodeInfoBase::RemoteShellCommand)
        .Default();
    registrar.BaseClassParameter("incarnation_id", &TNodeInfoBase::IncarnationId)
        .Default();
    registrar.BaseClassParameter("vcpu_factor", &TNodeInfoBase::VcpuFactor)
        .Default();
    registrar.BaseClassParameter("build_version", &TNodeInfoBase::BuildVersion)
        .Default();
    registrar.BaseClassParameter("binary_checksum", &TNodeInfoBase::BinaryChecksum)
        .Default();
    registrar.BaseClassParameter("flow_core_version", &TNodeInfoBase::FlowCoreVersion)
        .Default();
    registrar.BaseClassParameter("build_type", &TNodeInfoBase::BuildType)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
