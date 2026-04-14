#pragma once

#include <contrib/ydb/core/nbs/cloud/blockstore/config/protos/storage.pb.h>

#include <contrib/ydb/core/base/blobstorage.h>
#include <contrib/ydb/core/engine/minikql/flat_local_tx_factory.h>
#include <contrib/ydb/core/protos/blockstore_config.pb.h>

#include <contrib/ydb/library/actors/core/actor.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

NActors::IActor* CreatePartitionTablet(
    const NActors::TActorId& tablet,
    NKikimr::TTabletStorageInfo* info);

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
