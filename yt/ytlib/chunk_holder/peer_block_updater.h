#pragma once

#include "public.h"

// TODO(babenko): replace with public.h
#include <ytlib/actions/invoker.h>
#include <ytlib/misc/periodic_invoker.h>

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

class TChunkHolderServiceProxy;

class TPeerBlockUpdater
    : public TRefCounted
{
public:
    TPeerBlockUpdater(
        TChunkHolderConfig* config,
        TBlockStore* blockStore,
        IInvoker* invoker);

    void Start();
    void Stop();

private:
    void Update();
    
    typedef TChunkHolderServiceProxy TProxy;

    TChunkHolderConfigPtr Config;
    TBlockStorePtr BlockStore;
    TPeriodicInvoker::TPtr PeriodicInvoker;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
