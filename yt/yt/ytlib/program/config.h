#pragma once

#include <yt/yt/library/program/config.h>

#include <yt/yt/ytlib/auth/config.h>

#include <yt/yt/ytlib/chunk_client/config.h>

#include <yt/yt/library/containers/public.h>
#include <yt/yt/library/containers/disk_manager/public.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TNativeSingletonsConfig
    : public virtual TSingletonsConfig
{
public:
    NChunkClient::TDispatcherConfigPtr ChunkClientDispatcher;

    NAuth::TNativeAuthenticationManagerConfigPtr NativeAuthenticationManager;

    bool EnablePortoResourceTracker;
    NContainers::TPodSpecConfigPtr PodSpec;

    //! Configuration of the interaction with the host disk manager.
    NContainers::TDiskManagerProxyConfigPtr DiskManagerProxy;

    NContainers::TDiskInfoProviderConfigPtr DiskInfoProvider;

    REGISTER_YSON_STRUCT(TNativeSingletonsConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TNativeSingletonsConfig)

////////////////////////////////////////////////////////////////////////////////

class TNativeSingletonsDynamicConfig
    : public virtual TSingletonsDynamicConfig
{
public:
    NChunkClient::TDispatcherDynamicConfigPtr ChunkClientDispatcher;

    NAuth::TNativeAuthenticationManagerDynamicConfigPtr NativeAuthenticationManager;

    //! Configuration of the interaction with the host disk manager.
    NContainers::TDiskManagerProxyDynamicConfigPtr DiskManagerProxy;

    REGISTER_YSON_STRUCT(TNativeSingletonsDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TNativeSingletonsDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
