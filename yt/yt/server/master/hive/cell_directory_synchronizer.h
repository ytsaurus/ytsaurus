#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_server/public.h>

#include <yt/yt/server/lib/hydra/public.h>

#include <yt/yt/ytlib/hive/public.h>

#include <yt/yt/core/actions/future.h>

namespace NYT::NHiveServer {

////////////////////////////////////////////////////////////////////////////////

struct ICellDirectorySynchronizer
    : public TRefCounted
{
    virtual void Start() = 0;
    virtual void Stop() = 0;
};

DEFINE_REFCOUNTED_TYPE(ICellDirectorySynchronizer)

////////////////////////////////////////////////////////////////////////////////

ICellDirectorySynchronizerPtr CreateCellDirectorySynchronizer(
    TCellDirectorySynchronizerConfigPtr config,
    NHiveClient::ICellDirectoryPtr cellDirectory,
    NCellServer::ITamedCellManagerPtr cellManager,
    NHydra::IHydraManagerPtr hydraManager,
    IInvokerPtr automatonInvoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveServer
