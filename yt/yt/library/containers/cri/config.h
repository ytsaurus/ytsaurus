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
    //! Should be absolute and follow slice notation for systemd setup.
    TString BaseCgroup;

    //! Cpu quota period for cpu limits.
    TDuration CpuPeriod;

    //! By default at OOM kill all tasks at once. Requires cgroup-v2.
    bool MemoryOOMGroup;

    //! Retry requests on generic error with these message prefixes.
    std::vector<TString> RetryErrorPrefixes;

    REGISTER_YSON_STRUCT(TCriExecutorConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCriExecutorConfig)

////////////////////////////////////////////////////////////////////////////////

// TODO(khlebnikov): split docker registry stuff into common "docker" library.

//! TCriAuthConfig depicts docker registry authentification
class TCriAuthConfig
    : public NYTree::TYsonStruct
{
public:
    TString Username;

    TString Password;

    TString Auth;

    TString ServerAddress;

    TString IdentityToken;

    TString RegistryToken;

    REGISTER_YSON_STRUCT(TCriAuthConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCriAuthConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers::NCri
