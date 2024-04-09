#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NHiveServer {

////////////////////////////////////////////////////////////////////////////////

class TLogicalTimeRegistryConfig
    : public NYTree::TYsonStruct
{
public:
    TDuration EvictionPeriod;

    TDuration ExpirationTimeout;

    REGISTER_YSON_STRUCT(TLogicalTimeRegistryConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TLogicalTimeRegistryConfig)

////////////////////////////////////////////////////////////////////////////////

class THiveManagerConfig
    : public NYTree::TYsonStruct
{
public:
    //! Interval between consequent |Ping| requests to remote Hive Manager.
    TDuration PingPeriod;

    //! Interval between consequent idle (i.e. not carrying any payload) |PostMessages|
    //! requests to remote Hive Manager.
    TDuration IdlePostPeriod;

    //! Hive Manager will try to group post requests within this period.
    TDuration PostBatchingPeriod;

    //! Timeout for Ping RPC requests.
    TDuration PingRpcTimeout;

    //! Timeout for Send RPC requests.
    TDuration SendRpcTimeout;

    //! Timeout for Post RPC requests.
    TDuration PostRpcTimeout;

    //! Maximum number of messages to send via a single |PostMessages| request.
    int MaxMessagesPerPost;

    //! Maximum number of bytes to send via a single |PostMessages| request.
    i64 MaxBytesPerPost;

    //! Amount of time TMailbox is allowed to keep a cached channel.
    TDuration CachedChannelTimeout;

    //! Maximum time to wait before syncing with another instance.
    TDuration SyncDelay;

    //! Maximum time to wait before syncing with another instance.
    TDuration SyncTimeout;

    TLogicalTimeRegistryConfigPtr LogicalTimeRegistry;

    REGISTER_YSON_STRUCT(THiveManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(THiveManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TCellDirectorySynchronizerConfig
    : public NYTree::TYsonStruct
{
public:
    //! Interval between consequent SyncCells requests to the primary Hive Manager.
    TDuration SyncPeriod;

    REGISTER_YSON_STRUCT(TCellDirectorySynchronizerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCellDirectorySynchronizerConfig)

////////////////////////////////////////////////////////////////////////////////

class TClusterDirectorySynchronizerConfig
    : public NYTree::TYsonStruct
{
public:
    TDuration SyncPeriod;

    //! TTL for GetClusterMeta request.
    TDuration ExpireAfterSuccessfulUpdateTime;
    TDuration ExpireAfterFailedUpdateTime;

    REGISTER_YSON_STRUCT(TClusterDirectorySynchronizerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TClusterDirectorySynchronizerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveServer
