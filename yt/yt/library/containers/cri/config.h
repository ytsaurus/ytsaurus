#pragma once

#include "public.h"

#include <yt/yt/core/rpc/config.h>

namespace NYT::NContainers::NCri {

////////////////////////////////////////////////////////////////////////////////

class TCriExecutorConfig
    : public NRpc::TRetryingChannelConfig
{
public:
    //! gRPC endpoint for CRI container runtime service.
    TString RuntimeEndpoint;

    //! gRPC endpoint for CRI image manager service.
    TString ImageEndpoint;

    //! CRI namespace where this executor operates.
    TString Namespace;

    //! Name of CRI runtime configuration to use.
    TString RuntimeHandler;

    //! Common parent cgroup for all pods.
    TString BaseCgroup;

    //! Cpu quota period for cpu limits.
    TDuration CpuPeriod;

    REGISTER_YSON_STRUCT(TCriExecutorConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCriExecutorConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers::NCri
