#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/flow/library/cpp/common/sink.h>
#include <yt/yt/flow/library/cpp/connectors/common/delegating_async_sink_base.h>
#include <yt/yt/flow/library/cpp/connectors/common/ordered_batching_async_sink_base.h>
#include <yt/yt/flow/library/cpp/connectors/common/sync_sink_base.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EClickHouseCodec,
    (None)
    (Lz4)
    (Zstd)
);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EClickHouseHostSelectionPolicy,
    (OrderedRoundRobin)
    (RandomStart)
);

////////////////////////////////////////////////////////////////////////////////

struct TCommonClickHouseSinkParameters
    : public virtual NYTree::TYsonStruct
{
    std::string Host;
    ui16 Port = 9000;

    std::vector<std::string> Hosts;

    EClickHouseHostSelectionPolicy HostSelectionPolicy =
        EClickHouseHostSelectionPolicy::OrderedRoundRobin;

    THashMap<std::string, std::vector<std::string>> ShardHosts;

    std::vector<std::string> ShardingKeyColumns;

    std::string User;
    std::string PasswordEnvVar;

    std::string Database;
    std::string Table;

    EClickHouseCodec Codec = EClickHouseCodec::Lz4;

    bool EnableTls = false;
    std::vector<std::string> TlsCaFiles;
    std::string TlsCaDirectory;
    bool TlsSkipVerification = false;

    REGISTER_YSON_STRUCT(TCommonClickHouseSinkParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCommonClickHouseSinkParameters);

////////////////////////////////////////////////////////////////////////////////

//! Persisted shard topology of an exactly-once sink partition. Optional fields distinguish
//! a first start from state stamped by a sharding-aware binary.
struct TClickHouseShardTopologyState
    : public NYTree::TYsonStruct
{
    std::optional<std::string> TopologyFingerprint;
    std::optional<std::vector<std::string>> TargetIdentityFingerprints;

    REGISTER_YSON_STRUCT(TClickHouseShardTopologyState);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TClickHouseShardTopologyState);

////////////////////////////////////////////////////////////////////////////////

struct TDynamicCommonClickHouseSinkParameters
    : public virtual NYTree::TYsonStruct
{
    TDuration WriteTimeout;
    TDuration RetryBackoff;
    i64 MaxInsertAttempts = 10;

    bool AsyncInsert = false;

    TDuration ReplayHorizon;

    REGISTER_YSON_STRUCT(TDynamicCommonClickHouseSinkParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicCommonClickHouseSinkParameters);

////////////////////////////////////////////////////////////////////////////////

struct TClickHouseBatchingSinkBaseParameters
    : public TOrderedBatchingAsyncSinkBase::TParameters
    , public virtual TCommonClickHouseSinkParameters
{
    REGISTER_YSON_STRUCT(TClickHouseBatchingSinkBaseParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TClickHouseBatchingSinkBaseParameters);

struct TClickHouseBatchingSinkParameters
    : public TClickHouseBatchingSinkBaseParameters
{
    REGISTER_YSON_STRUCT(TClickHouseBatchingSinkParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TClickHouseBatchingSinkParameters);

////////////////////////////////////////////////////////////////////////////////

struct TDynamicClickHouseBatchingSinkParameters
    : public TOrderedBatchingAsyncSinkBase::TDynamicParameters
    , public virtual TDynamicCommonClickHouseSinkParameters
{
    REGISTER_YSON_STRUCT(TDynamicClickHouseBatchingSinkParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicClickHouseBatchingSinkParameters);

struct TShardedClickHouseBatchingSinkParameters
    : public TClickHouseBatchingSinkBaseParameters
{
    REGISTER_YSON_STRUCT(TShardedClickHouseBatchingSinkParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TShardedClickHouseBatchingSinkParameters);

struct TDynamicShardedClickHouseBatchingSinkParameters
    : public TDynamicClickHouseBatchingSinkParameters
{
    REGISTER_YSON_STRUCT(TDynamicShardedClickHouseBatchingSinkParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicShardedClickHouseBatchingSinkParameters);

////////////////////////////////////////////////////////////////////////////////

struct TAtLeastOnceClickHouseSinkParameters
    : public TSyncSinkBase::TParameters
    , public virtual TCommonClickHouseSinkParameters
{
    REGISTER_YSON_STRUCT(TAtLeastOnceClickHouseSinkParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TAtLeastOnceClickHouseSinkParameters);

////////////////////////////////////////////////////////////////////////////////

struct TDynamicAtLeastOnceClickHouseSinkParameters
    : public TSyncSinkBase::TDynamicParameters
    , public virtual TDynamicCommonClickHouseSinkParameters
{
    REGISTER_YSON_STRUCT(TDynamicAtLeastOnceClickHouseSinkParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicAtLeastOnceClickHouseSinkParameters);

////////////////////////////////////////////////////////////////////////////////

struct TAtMostOnceClickHouseSinkParameters
    : public TDelegatingAsyncSinkParameters
    , public virtual TCommonClickHouseSinkParameters
{
    REGISTER_YSON_STRUCT(TAtMostOnceClickHouseSinkParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TAtMostOnceClickHouseSinkParameters);

////////////////////////////////////////////////////////////////////////////////

struct TDynamicAtMostOnceClickHouseSinkParameters
    : public TDelegatingAsyncSinkDynamicParameters
    , public virtual TDynamicCommonClickHouseSinkParameters
{
    REGISTER_YSON_STRUCT(TDynamicAtMostOnceClickHouseSinkParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicAtMostOnceClickHouseSinkParameters);

////////////////////////////////////////////////////////////////////////////////

struct TClickHouseSinkControllerParameters
    : public virtual ISink::TParameters
    , public virtual TCommonClickHouseSinkParameters
{
    REGISTER_YSON_STRUCT(TClickHouseSinkControllerParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TClickHouseSinkControllerParameters);

////////////////////////////////////////////////////////////////////////////////

struct TDynamicClickHouseSinkControllerParameters
    : public virtual ISink::TDynamicParameters
    , public virtual TDynamicCommonClickHouseSinkParameters
{
    REGISTER_YSON_STRUCT(TDynamicClickHouseSinkControllerParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicClickHouseSinkControllerParameters);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
