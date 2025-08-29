#pragma once

#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/core/base/blobstorage.h>

namespace NKikimr::NStat {

IActor* CreateStatisticsAggregator(const NActors::TActorId& tablet, TTabletStorageInfo* info);

} // NKikimr::NStat
