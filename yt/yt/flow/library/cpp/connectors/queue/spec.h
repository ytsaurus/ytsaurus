#pragma once

#include "public.h"

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/ypath/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/flow/library/cpp/connectors/common/delegating_async_sink_base.h>
#include <yt/yt/flow/library/cpp/connectors/common/ordered_source_base.h>
#include <yt/yt/flow/library/cpp/connectors/common/sync_sink_base.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TQueueInfoSpec
    : public virtual NYTree::TYsonStruct
{
    NYPath::TRichYPath QueuePath;
    TDuration UpdatePartitionCountPeriod;
    TDuration UpdatePartitionCountRetryMinBackoff;

    REGISTER_YSON_STRUCT(TQueueInfoSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TQueueInfoSpec);

////////////////////////////////////////////////////////////////////////////////

struct TQueueSourceParameters
    : public TOrderedSourceBase::TParameters
    , public TQueueInfoSpec
{
    NYPath::TRichYPath ConsumerPath;

    bool TryParseFlowQueueMeta{};
    std::string FlowQueueMetaColumn;
    bool IgnoreMalformedFlowQueueMeta{};

    std::optional<std::vector<std::pair<int, int>>> PartitionFilter;

    REGISTER_YSON_STRUCT(TQueueSourceParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TQueueSourceParameters);

////////////////////////////////////////////////////////////////////////////////

struct TDynamicQueueSourceParameters
    : public TOrderedSourceBase::TDynamicParameters
{
    TDuration PullQueueTimeout;

    REGISTER_YSON_STRUCT(TDynamicQueueSourceParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicQueueSourceParameters);

////////////////////////////////////////////////////////////////////////////////

struct TCommonQueueSinkParameters
    : public TQueueInfoSpec
{
    bool WriteFlowQueueMeta{};
    std::string FlowQueueMetaColumn;

    REGISTER_YSON_STRUCT(TCommonQueueSinkParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCommonQueueSinkParameters);

////////////////////////////////////////////////////////////////////////////////

struct TDynamicCommonQueueSinkParameters
    : public virtual NYTree::TYsonStruct
{
    TDuration FlowQueueMetaHeartbeatPeriod;

    REGISTER_YSON_STRUCT(TDynamicCommonQueueSinkParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicCommonQueueSinkParameters);

////////////////////////////////////////////////////////////////////////////////

//! Optional expression-based routing of sync queue-sink rows to tablets via
//! YT's column evaluator (builtins only). Neither expression set => no
//! $tablet_index is written (today's behavior).
struct TQueueSinkTabletRoutingParameters
    : public virtual NYTree::TYsonStruct
{
    //! Verbatim mode: expression whose value is written to $tablet_index verbatim.
    //! Mutually exclusive with |TabletIndexRoutingHashExpression|; unset both => routing off.
    std::optional<std::string> TabletIndexExpression;

    //! Hash mode: expression yielding a uint64 hash reduced to a tablet index by
    //! |TabletIndexRoutingHashPolicy| over |TabletCount|.
    std::optional<std::string> TabletIndexRoutingHashExpression;

    //! Required with |TabletIndexRoutingHashExpression|; the hash reduction policy.
    std::optional<EQueueTabletIndexRoutingHashPolicy> TabletIndexRoutingHashPolicy;

    //! Optional for the hash mode; when unset the target queue's @tablet_count is resolved at init.
    std::optional<i64> TabletCount;

    REGISTER_YSON_STRUCT(TQueueSinkTabletRoutingParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TQueueSinkTabletRoutingParameters);

////////////////////////////////////////////////////////////////////////////////

struct TSyncQueueSinkParameters
    : public TSyncSinkBase::TParameters
    , public virtual TCommonQueueSinkParameters
    , public virtual TQueueSinkTabletRoutingParameters
{
    std::optional<THashSet<std::string>> ColumnFilter;

    REGISTER_YSON_STRUCT(TSyncQueueSinkParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSyncQueueSinkParameters);

////////////////////////////////////////////////////////////////////////////////

struct TDynamicSyncQueueSinkParameters
    : public TSyncSinkBase::TDynamicParameters
    , public virtual TDynamicCommonQueueSinkParameters
{
    REGISTER_YSON_STRUCT(TDynamicSyncQueueSinkParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicSyncQueueSinkParameters);

////////////////////////////////////////////////////////////////////////////////

struct TAsyncQueueWriterParametersBase
    : public virtual TCommonQueueSinkParameters
{
    NYPath::TRichYPath ProducerPath;
    bool RequireSyncReplica{};

    REGISTER_YSON_STRUCT(TAsyncQueueWriterParametersBase);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

struct TAsyncQueueWriterParameters
    : public virtual TAsyncQueueWriterParametersBase
{
    REGISTER_YSON_STRUCT(TAsyncQueueWriterParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TAsyncQueueWriterParameters);

////////////////////////////////////////////////////////////////////////////////

struct TAsyncMultiClusterQueueWriterParameters
    : public virtual TAsyncQueueWriterParametersBase
{
    bool UseClusters{};

    REGISTER_YSON_STRUCT(TAsyncMultiClusterQueueWriterParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TAsyncMultiClusterQueueWriterParameters);

////////////////////////////////////////////////////////////////////////////////

struct TDynamicAsyncQueueWriterParameters
    : public virtual TDynamicCommonQueueSinkParameters
{
    TDuration WritePeriod;
    i64 MaxRowsPerWrite{};
    i64 MaxBytesPerWrite{};

    TDuration BackoffDuration;

    REGISTER_YSON_STRUCT(TDynamicAsyncQueueWriterParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicAsyncQueueWriterParameters);

////////////////////////////////////////////////////////////////////////////////

//! Routing is not supported on async queue sinks yet (tracked by YTFLOW-766). The routing params
//! are mixed into the async spec only so they are recognized and this rejects them loudly instead
//! of silently ignoring them.
void ValidateAsyncSinkTabletRoutingUnsupported(const TQueueSinkTabletRoutingParameters& parameters);

////////////////////////////////////////////////////////////////////////////////

struct TAsyncQueueSinkParametersBase
    : public TDelegatingAsyncSinkBase::TParameters
    , public virtual TAsyncQueueWriterParametersBase
    , public virtual TQueueSinkTabletRoutingParameters
{
    std::optional<THashSet<std::string>> ColumnFilter;

    REGISTER_YSON_STRUCT(TAsyncQueueSinkParametersBase);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TAsyncQueueSinkParametersBase);

////////////////////////////////////////////////////////////////////////////////

struct TAsyncQueueSinkParameters
    : public TAsyncQueueSinkParametersBase
    , public virtual TAsyncQueueWriterParameters
{
    REGISTER_YSON_STRUCT(TAsyncQueueSinkParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TAsyncQueueSinkParameters);

////////////////////////////////////////////////////////////////////////////////

struct TAsyncMultiClusterQueueSinkParameters
    : public TAsyncQueueSinkParametersBase
    , public TAsyncMultiClusterQueueWriterParameters
{
    REGISTER_YSON_STRUCT(TAsyncMultiClusterQueueSinkParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TAsyncMultiClusterQueueSinkParameters);

////////////////////////////////////////////////////////////////////////////////

struct TDynamicAsyncQueueSinkParameters
    : public TDelegatingAsyncSinkBase::TDynamicParameters
    , public TDynamicAsyncQueueWriterParameters
{
    REGISTER_YSON_STRUCT(TDynamicAsyncQueueSinkParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicAsyncQueueSinkParameters);

////////////////////////////////////////////////////////////////////////////////

struct TQueueSinkControllerParameters
    : public virtual ISink::TParameters
    , public virtual TCommonQueueSinkParameters
{
    REGISTER_YSON_STRUCT(TQueueSinkControllerParameters);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

struct TDynamicQueueSinkControllerParameters
    : public virtual ISink::TDynamicParameters
    , public virtual TDynamicCommonQueueSinkParameters
{
    REGISTER_YSON_STRUCT(TDynamicQueueSinkControllerParameters);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
