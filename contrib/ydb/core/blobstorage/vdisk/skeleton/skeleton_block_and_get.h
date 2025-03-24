#pragma once

#include <contrib/ydb/core/blobstorage/vdisk/common/vdisk_context.h>
#include <contrib/ydb/core/blobstorage/vdisk/common/vdisk_events.h>

#include <contrib/ydb/library/actors/core/actor.h>

#include <memory>

namespace NKikimr {

std::unique_ptr<NActors::IActor> CreateBlockAndGetActor(
    TEvBlobStorage::TEvVGet::TPtr ev,
    NActors::TActorId skeletonId,
    TIntrusivePtr<TVDiskContext> vCtx,
    TActorIDPtr skeletonFrontIDPtr,
    TVDiskID selfVDiskId,
    TVDiskIncarnationGuid vDiskIncarnationGuid,
    TIntrusivePtr<NKikimr::TBlobStorageGroupInfo> gInfo);

} // NKikimr
