#pragma once
#include "defs.h"

#include <contrib/ydb/core/base/blobstorage.h>

namespace NKikimr {
namespace NSequenceShard {

    IActor* CreateSequenceShard(const TActorId& tablet, TTabletStorageInfo* info);

} // namespace NSequenceShard
} // namespace NKikimr
