#pragma once

#include "public.h"

#include <yt/server/tablet_server/public.h>

#include <yt/server/hydra/public.h>

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
        NTabletServer::TTabletManagerPtr tabletManager,
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
