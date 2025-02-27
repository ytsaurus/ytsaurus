#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NHiveServer {

////////////////////////////////////////////////////////////////////////////////

struct TLogicalTimeRegistryConfig
    : public NYTree::TYsonStruct
{
    TDuration EvictionPeriod;
    TDuration ExpirationTimeout;

    REGISTER_YSON_STRUCT(TLogicalTimeRegistryConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TLogicalTimeRegistryConfig)

////////////////////////////////////////////////////////////////////////////////

struct THiveManagerConfig
    : public NYTree::TYsonStruct
{
    // COMPAT(babenko)
    bool UseNew;

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

    //! Interval between readonly checks.
    TDuration ReadOnlyCheckPeriod;

    //! If true, trace contexts are sent with baggage.
    bool SendTracingBaggage;

    TLogicalTimeRegistryConfigPtr LogicalTimeRegistry;

    THashSet<NObjectClient::TCellTag> AllowedForRemovalMasterCellTags;

    REGISTER_YSON_STRUCT(THiveManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(THiveManagerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TCellDirectorySynchronizerConfig
    : public NYTree::TYsonStruct
{
    //! Interval between consequent SyncCells requests to the primary Hive Manager.
    TDuration SyncPeriod;

    REGISTER_YSON_STRUCT(TCellDirectorySynchronizerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCellDirectorySynchronizerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TClusterDirectorySynchronizerConfig
    : public NYTree::TYsonStruct
{
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
