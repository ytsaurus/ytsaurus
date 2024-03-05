#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_server/public.h>

#include <yt/yt/server/lib/hydra/public.h>

#include <yt/yt/ytlib/hive/public.h>

#include <yt/yt/core/actions/future.h>

namespace NYT::NHiveServer {

////////////////////////////////////////////////////////////////////////////////


NHiveClient::ICellDirectorySynchronizerPtr CreateCellDirectorySynchronizer(
    TCellDirectorySynchronizerConfigPtr config,
    NHiveClient::ICellDirectoryPtr cellDirectory,
    NCellServer::ITamedCellManagerPtr cellManager,
    NHydra::IHydraManagerPtr hydraManager,
    IInvokerPtr automatonInvoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveServer
