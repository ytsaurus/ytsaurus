#include "partition_direct_actor.h"

#include <contrib/ydb/core/base/appdata_fwd.h>

#include <contrib/ydb/library/actors/core/actor.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

IActor* CreatePartitionTablet(const TActorId& tablet, TTabletStorageInfo* info)
{
    return new TPartitionActor(tablet, info);
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
