#pragma once

#include "public.h"

// #include <ydb/core/nbs/cloud/blockstore/libs/diagnostics/public.h>
// #include <ydb/core/nbs/cloud/blockstore/libs/endpoints/public.h>
#include <contrib/ydb/core/nbs/cloud/blockstore/libs/kikimr/public.h>
// #include <ydb/core/nbs/cloud/blockstore/libs/rdma/iface/public.h>
#include <contrib/ydb/core/nbs/cloud/blockstore/libs/storage/core/public.h>

#include <contrib/ydb/core/base/blobstorage.h>

#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/actors/core/actorid.h>

namespace NYdb::NBS::NStorage {

using namespace NActors;
using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

enum class EVolumeStartMode
{
    ONLINE,
    MOUNTED
};

////////////////////////////////////////////////////////////////////////////////

IActor* CreateVolumeTablet(const TActorId& tablet, TTabletStorageInfo* info);

}   // namespace NYdb::NBS::NStorage
