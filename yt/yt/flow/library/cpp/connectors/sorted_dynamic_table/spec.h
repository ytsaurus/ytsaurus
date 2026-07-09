#pragma once

#include <library/cpp/yt/memory/ref_counted.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/ypath/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/flow/library/cpp/connectors/common/ordered_async_sink_base.h>
#include <yt/yt/flow/library/cpp/connectors/common/sync_sink_base.h>

namespace NYT::NFlow::NSortedDynamicTable {

////////////////////////////////////////////////////////////////////////////////

struct TInfoSpec
    : public virtual NYTree::TYsonStruct
{
    NYPath::TRichYPath TablePath;
    TDuration UpdatePartitionCountPeriod;

    REGISTER_YSON_STRUCT(TInfoSpec);

    static void Register(TRegistrar registrar);
};

DECLARE_REFCOUNTED_TYPE(TInfoSpec);

////////////////////////////////////////////////////////////////////////////////

struct TSyncSinkParameters
    : public TSyncSinkBase::TParameters
    , public virtual TInfoSpec
{
    std::optional<THashSet<std::string>> ColumnFilter;
    std::optional<THashSet<std::string>> AggregateColumns;
    bool DeleteRows{};
    bool RequireSyncReplica{};

    REGISTER_YSON_STRUCT(TSyncSinkParameters);

    static void Register(TRegistrar registrar);
};

DECLARE_REFCOUNTED_TYPE(TSyncSinkParameters);

////////////////////////////////////////////////////////////////////////////////

struct TDynamicSyncSinkParameters
    : public TSyncSinkBase::TDynamicParameters
{
    REGISTER_YSON_STRUCT(TDynamicSyncSinkParameters);

    static void Register(TRegistrar registrar);
};

DECLARE_REFCOUNTED_TYPE(TDynamicSyncSinkParameters);

////////////////////////////////////////////////////////////////////////////////

struct TSinkControllerParameters
    : public virtual ISink::TParameters
    , public virtual TInfoSpec
{
    REGISTER_YSON_STRUCT(TSinkControllerParameters);

    static void Register(TRegistrar registrar);
};

DECLARE_REFCOUNTED_TYPE(TSinkControllerParameters);

////////////////////////////////////////////////////////////////////////////////

struct TDynamicSinkControllerParameters
    : public virtual ISink::TDynamicParameters
{
    REGISTER_YSON_STRUCT(TDynamicSinkControllerParameters);

    static void Register(TRegistrar registrar);
};

DECLARE_REFCOUNTED_TYPE(TDynamicSinkControllerParameters);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NSortedDynamicTable
