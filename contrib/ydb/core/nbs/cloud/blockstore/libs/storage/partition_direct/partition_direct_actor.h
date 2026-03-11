#pragma once

#include <contrib/ydb/core/nbs/cloud/blockstore/config/storage.pb.h>
#include <contrib/ydb/core/nbs/cloud/blockstore/libs/storage/api/service.h>

#include <contrib/ydb/core/blobstorage/base/blobstorage_events.h>
#include <contrib/ydb/core/protos/blockstore_config.pb.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

class TPartitionActor
    : public NActors::TActorBootstrapped<TPartitionActor>
{
private:
    NYdb::NBS::NProto::TStorageConfig StorageConfig;
    NKikimrBlockStore::TVolumeConfig VolumeConfig;

    NActors::TActorId BSControllerPipeClient;

    NActors::TActorId LoadActorAdapter;


public:
    TPartitionActor(
        NYdb::NBS::NProto::TStorageConfig storageConfig,
        NKikimrBlockStore::TVolumeConfig volumeConfig);

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    STFUNC(StateWork);

    void CreateBSControllerPipeClient(const NActors::TActorContext& ctx);

    void AllocateDDiskBlockGroup(const NActors::TActorContext& ctx);

    void HandleControllerAllocateDDiskBlockGroupResult(
        const NKikimr::TEvBlobStorage::TEvControllerAllocateDDiskBlockGroupResult::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleGetLoadActorAdapterActorId(
        const NYdb::NBS::NBlockStore::TEvService::TEvGetLoadActorAdapterActorIdRequest::TPtr& ev,
        const NActors::TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

}  // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
