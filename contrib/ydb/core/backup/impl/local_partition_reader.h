#pragma once

#include <contrib/ydb/library/actors/core/actor.h>

namespace NKikimr::NBackup::NImpl {

NActors::IActor* CreateLocalPartitionReader(const NActors::TActorId& PQTabletMbox, ui32 partition);

} // namespace NKikimr::NBackup::NImpl
