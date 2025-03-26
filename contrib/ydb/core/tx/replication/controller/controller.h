#pragma once

#include <contrib/ydb/core/base/blobstorage.h>
#include <contrib/ydb/core/base/defs.h>

namespace NKikimr::NReplication {

IActor* CreateController(const TActorId& tablet, TTabletStorageInfo* info);

}
