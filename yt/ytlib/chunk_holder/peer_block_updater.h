#pragma once

#include "config.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TPeriodicInvoker;
struct IInvoker;

////////////////////////////////////////////////////////////////////////////////

namespace NChunkHolder {

class TBlockStore;
class TChunkHolderServiceProxy;

class TPeerBlockUpdater
    : public TRefCounted
{
public:
    typedef TIntrusivePtr<TPeerBlockUpdater> TPtr;

    TPeerBlockUpdater(
        TChunkHolderConfig* config,
        TBlockStore* blockStore,
        IInvoker* invoker);

    void Start();
    void Stop();

private:
    void Update();
    
    typedef TChunkHolderServiceProxy TProxy;

    TChunkHolderConfig::TPtr Config;
    TIntrusivePtr<TBlockStore> BlockStore;
    TIntrusivePtr<TPeriodicInvoker> PeriodicInvoker;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
