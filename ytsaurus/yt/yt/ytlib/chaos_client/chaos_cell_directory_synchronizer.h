#pragma once

#include "public.h"

#include <yt/yt/ytlib/hive/public.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/logging/public.h>

#include <yt/yt/client/object_client/public.h>

namespace NYT::NChaosClient {

////////////////////////////////////////////////////////////////////////////////

struct IChaosCellDirectorySynchronizer
    : public TRefCounted
{
    virtual void AddCellIds(const std::vector<NObjectClient::TCellId>& cellIds) = 0;
    virtual void AddCellTag(NObjectClient::TCellTag) = 0;
    virtual TFuture<void> Sync() = 0;
    virtual void Start() = 0;
    virtual void Stop() = 0;
};

DEFINE_REFCOUNTED_TYPE(IChaosCellDirectorySynchronizer)

////////////////////////////////////////////////////////////////////////////////

IChaosCellDirectorySynchronizerPtr CreateChaosCellDirectorySynchronizer(
    TChaosCellDirectorySynchronizerConfigPtr,
    NHiveClient::ICellDirectoryPtr cellDirectory,
    NApi::NNative::IConnectionPtr connection,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient
