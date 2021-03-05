#pragma once

#include "public.h"

#include <yt/yt/ytlib/hive/public.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/ytlib/api/native/public.h>

namespace NYT::NTabletClient {

////////////////////////////////////////////////////////////////////////////////

ITableMountCachePtr CreateNativeTableMountCache(
    TTableMountCacheConfigPtr config,
    NApi::NNative::IConnectionPtr connection,
    NHiveClient::TCellDirectoryPtr cellDirectory,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletClient

