#pragma once

#include <contrib/ydb/core/base/blobstorage.h>

namespace NKikimr {

IActor* CreateFlatBsController(const TActorId &tablet, TTabletStorageInfo *info);

} //NKikimr
