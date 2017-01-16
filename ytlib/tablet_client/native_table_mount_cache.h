#pragma once

#include "public.h"

#include <yt/ytlib/hive/public.h>

#include <yt/core/rpc/public.h>

namespace NYT {
namespace NTabletClient {

///////////////////////////////////////////////////////////////////////////////

ITableMountCachePtr CreateNativeTableMountCache(
    TTableMountCacheConfigPtr config,
    NRpc::IChannelPtr masterChannel,
    NHiveClient::TCellDirectoryPtr cellDirectory);

///////////////////////////////////////////////////////////////////////////////

} // namespace NTabletClient
} // namespace NYT

