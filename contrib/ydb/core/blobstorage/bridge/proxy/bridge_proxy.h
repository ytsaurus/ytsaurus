#pragma once

#include "defs.h"

#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>

namespace NKikimr {

    IActor *CreateBridgeProxyActor(TIntrusivePtr<TBlobStorageGroupInfo> info);

} // NKikimr
