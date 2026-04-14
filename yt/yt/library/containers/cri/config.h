#pragma once

#include "public.h"

#include <yt/yt/core/misc/cache_config.h>

#include <yt/yt/core/rpc/config.h>

#include <yt/yt/library/re2/public.h>

namespace NYT::NContainers::NCri {

////////////////////////////////////////////////////////////////////////////////

struct TCriExecutorConfig
    : public NRpc::TRetryingChannelConfig
{
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
    bool MemoryOomGroup;

    //! Retry requests on generic error with these message prefixes.
    std::vector<std::string> RetryErrorPrefixes;

    //! Retry requests on generic error with matched message.
    NRe2::TRe2Ptr RetryErrorPattern;

    REGISTER_YSON_STRUCT(TCriExecutorConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCriExecutorConfig)

////////////////////////////////////////////////////////////////////////////////

// TODO(khlebnikov): split docker registry stuff into common "docker" library.

//! TCriAuthConfig depicts docker registry authentification
struct TCriAuthConfig
    : public NYTree::TYsonStruct
{
    std::string Username;

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

struct TCriImageCacheConfig
    : public TSlruCacheConfig
{
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

struct TCriPodDescriptor
    : public NYTree::TYsonStruct
{
    std::string Name;
    std::string Id;

    static TCriPodDescriptorPtr Create(std::string name, std::string id);

    REGISTER_YSON_STRUCT(TCriPodDescriptor);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCriPodDescriptor)

////////////////////////////////////////////////////////////////////////////////

struct TCriContainerResources
    : public NYTree::TYsonStructLite
{
    std::optional<double> CpuLimit;
    std::optional<double> CpuRequest;
    std::optional<i64> MemoryLimit;
    std::optional<i64> MemoryRequest;

    //! At OOM kill all tasks at once.
    std::optional<bool> MemoryOomGroup;

    std::optional<std::string> CpusetCpus;

    REGISTER_YSON_STRUCT_LITE(TCriContainerResources);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

struct TCriPodSpec
    : public NYTree::TYsonStruct
{
    std::string Name;
    TCriContainerResources Resources;

    REGISTER_YSON_STRUCT(TCriPodSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCriPodSpec)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers::NCri
