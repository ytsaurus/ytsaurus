#pragma once

#include <contrib/ydb/library/actors/core/actorsystem_fwd.h>


namespace NKikimr {

class TTabletStorageInfo;

NActors::IActor* CreatePersQueueReadBalancer(const NActors::TActorId& tablet, TTabletStorageInfo *info);

} // namespace NKikimr
