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

struct TSyncQueueSinkParameters
    : public TSyncSinkBase::TParameters
    , public virtual TCommonQueueSinkParameters
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

struct TAsyncQueueSinkParametersBase
    : public TDelegatingAsyncSinkBase::TParameters
    , public virtual TAsyncQueueWriterParametersBase
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
