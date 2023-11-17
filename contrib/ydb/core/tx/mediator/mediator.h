#pragma once
#include "defs.h"

#include <contrib/ydb/core/base/blobstorage.h>

namespace NKikimr {

IActor* CreateTxMediator(const TActorId &tablet, TTabletStorageInfo *info);

}
