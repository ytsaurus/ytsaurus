#pragma once

#include "public.h"

// TODO(babenko): replace with public.h
#include <ytlib/actions/bind.h>
#include <ytlib/actions/invoker.h>

#include <ytlib/misc/periodic_invoker.h>

#include <ytlib/chunk_client/data_node_service_proxy.h>

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

class TPeerBlockUpdater
    : public TRefCounted
{
public:
    TPeerBlockUpdater(
        TDataNodeConfigPtr config,
        TBootstrap* bootstrap);

    void Start();
    void Stop();

private:
    void Update();
    
    typedef NChunkClient::TDataNodeServiceProxy TProxy;

    TDataNodeConfigPtr Config;
    TBootstrap* Bootstrap;
    TPeriodicInvokerPtr PeriodicInvoker;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
