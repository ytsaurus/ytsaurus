#pragma once

#include <contrib/ydb/core/kqp/runtime/kqp_compute.h>
#include <contrib/ydb/core/protos/sys_view_types.pb.h>

#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/actors/core/actorid.h>

namespace NKikimr {
namespace NSysView {

struct TQueryStatsBucketRange {
    ui64 FromBucket = 0;
    TMaybe<ui32> FromRank;

    ui64 ToBucket = std::numeric_limits<ui64>::max();
    TMaybe<ui32> ToRank;

    bool IsEmpty = false;

    explicit TQueryStatsBucketRange(const TSerializedTableRange& range, const TDuration& bucketSize);
};

THolder<NActors::IActor> CreateQueryStatsScan(const NActors::TActorId& ownerId, ui32 scanId, const TTableId& tableId,
    const NKikimrSysView::ESysViewType sysViewType, const TTableRange& tableRange,
    const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns);

} // NSysView
} // NKikimr
