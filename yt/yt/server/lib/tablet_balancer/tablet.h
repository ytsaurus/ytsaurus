#pragma once

#include "public.h"

#include <yt/yt/ytlib/tablet_client/proto/master_tablet_service.pb.h>

#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/ytree/public.h>

#include <library/cpp/yt/yson_string/string.h>

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

struct TTabletStatistics
{
    i64 CompressedDataSize;
    i64 UncompressedDataSize;
    i64 MemorySize;

    int PartitionCount;

    NYTree::INodePtr OriginalNode;
};

////////////////////////////////////////////////////////////////////////////////

struct TTablet final
{
    using TPerformanceCountersProtoList = NProtoBuf::RepeatedPtrField<
        NTabletClient::NProto::TRspGetTableBalancingAttributes::TTablet::TEmaCounterSnapshot>;

    const TTabletId Id;
    const TTable* Table;

    i64 Index = {};
    TWeakPtr<TTabletCell> Cell = nullptr;
    TInstant MountTime;

    TTabletStatistics Statistics;

    // TYsonString is only used in tests.
    std::variant<
        TPerformanceCountersProtoList,
        NYson::TYsonString,
        NTableClient::TUnversionedOwningRow> PerformanceCounters;
    ETabletState State = ETabletState::Unmounted;

    TTablet(
        TTabletId tabletId,
        TTable* table);

    NYson::TYsonString GetPerformanceCountersYson(
        const std::vector<TString>& performanceCountersKeys,
        const NTableClient::TTableSchemaPtr& performanceCountersTableSchema) const;
};

DEFINE_REFCOUNTED_TYPE(TTablet)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
