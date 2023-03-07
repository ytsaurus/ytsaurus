#pragma once

#include "public.h"

#include <yt/server/master/cell_server/public.h>

#include <yt/server/lib/hydra/public.h>

#include <yt/ytlib/hive/public.h>

#include <yt/core/actions/future.h>

namespace NYT::NHiveServer {

////////////////////////////////////////////////////////////////////////////////

class TCellDirectorySynchronizer
    : public TRefCounted
{
public:
    TCellDirectorySynchronizer(
        TCellDirectorySynchronizerConfigPtr config,
        NHiveClient::TCellDirectoryPtr cellDirectory,
        NCellServer::TTamedCellManagerPtr cellManager,
        NHydra::IHydraManagerPtr hydraManager,
        IInvokerPtr automatonInvoker);
    ~TCellDirectorySynchronizer();

    void Start();
    void Stop();

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(TCellDirectorySynchronizer)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveServer
