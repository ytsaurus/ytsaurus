#pragma once

#include "public.h"

#include <yt/yt/core/misc/cache_config.h>

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

class TCriImageCacheConfig
    : public TSlruCacheConfig
{
public:
    //! Manage only images with these prefixes, except images explicitly marked
    //! as unmanaged. Present unmanaged images could be used, but they are not
    //! accounted and never removed or pulled from/into cache on demand.
    std::vector<TString> ManagedPrefixes;

    //! Never pull or remove images with these prefixes.
    std::vector<TString> UnmanagedPrefixes;

    //! List of images which must be prefetched and kept in cache.
    std::vector<TString> PinnedImages;

    //! Initial estimation for space required for pulling image into cache.
    i64 ImageSizeEstimation;

    //! Multiplier for image size to account space used by unpacked images.
    //! Workaround for: https://github.com/containerd/containerd/issues/9261
    double ImageCompressionRatioEstimation;

    //! Always pull image with tag "latest".
    bool AlwaysPullLatest;

    //! Pull images periodically.
    TDuration PullPeriod;

    REGISTER_YSON_STRUCT(TCriImageCacheConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCriImageCacheConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers::NCri
