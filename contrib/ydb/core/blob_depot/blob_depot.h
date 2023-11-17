#pragma once

#include <contrib/ydb/core/base/blobstorage.h>

namespace NKikimr::NBlobDepot {

    IActor *CreateBlobDepot(const TActorId& tablet, TTabletStorageInfo *info);

} // NKikimr::NBlobDepot
