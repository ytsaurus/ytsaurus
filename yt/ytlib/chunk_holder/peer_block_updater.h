#pragma once

#include "public.h"

// TODO(babenko): replace with public.h
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
        TChunkHolderConfigPtr config,
        TBootstrap* bootstrap);

    void Start();
    void Stop();

private:
    void Update();
    
    typedef TChunkHolderServiceProxy TProxy;

    TChunkHolderConfigPtr Config;
    TBootstrap* Bootstrap;
    TPeriodicInvoker::TPtr PeriodicInvoker;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
