#pragma once

#include "public.h"

// TODO(babenko): replace with public.h
#include <ytlib/actions/bind.h>
#include <ytlib/actions/invoker.h>
#include <ytlib/misc/periodic_invoker.h>

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
    
    typedef TChunkHolderServiceProxy TProxy;

    TDataNodeConfigPtr Config;
    TBootstrap* Bootstrap;
    TPeriodicInvoker::TPtr PeriodicInvoker;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
