#pragma once

#include <contrib/ydb/core/kqp/runtime/kqp_compute.h>

#include <contrib/ydb/core/protos/sys_view_types.pb.h>
#include <contrib/ydb/library/actors/core/actor.h>

namespace NKikimr {
namespace NSysView {

THolder<NActors::IActor> CreateTabletsScan(const NActors::TActorId& ownerId, ui32 scanId,
    const NKikimrSysView::TSysViewDescription& sysViewInfo, const TTableRange& tableRange,
    const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns);

} // NSysView
} // NKikimr
