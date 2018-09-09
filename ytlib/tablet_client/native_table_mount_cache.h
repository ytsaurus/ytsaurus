#pragma once

#include "public.h"

#include <yt/ytlib/hive/public.h>

#include <yt/core/rpc/public.h>

#include <yt/core/logging/log.h>

#include <yt/ytlib/api/native/public.h>

namespace NYT {
namespace NTabletClient {

////////////////////////////////////////////////////////////////////////////////

ITableMountCachePtr CreateNativeTableMountCache(
    TTableMountCacheConfigPtr config,
    NApi::NNative::IConnectionPtr connection,
    NHiveClient::TCellDirectoryPtr cellDirectory,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletClient
} // namespace NYT

