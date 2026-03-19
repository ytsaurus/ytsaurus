#pragma once

#include "public.h"

#include <contrib/ydb/core/nbs/cloud/blockstore/libs/kikimr/public.h>
#include <contrib/ydb/core/nbs/cloud/blockstore/libs/storage/core/public.h>

#include <contrib/ydb/core/nbs/cloud/storage/core/libs/actors/public.h>

namespace NYdb::NBS::NStorage {

////////////////////////////////////////////////////////////////////////////////

NActors::IActorPtr CreateSSProxy(
    const NProto::TStorageServiceConfig& nbsStorageConfig);

}   // namespace NYdb::NBS::NStorage
