#pragma once

#include "common.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TPeriodicInvoker;
struct IInvoker;
class NRpc::TChannelCache;

////////////////////////////////////////////////////////////////////////////////

namespace NChunkHolder {

class TBlockStore;
class TChunkHolderServiceProxy;

class TPeerUpdater
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TPeerUpdater> TPtr;

    TPeerUpdater(
        TChunkHolderConfig* config,
        TBlockStore* blockStore,
        NRpc::TChannelCache* channelCache,
        IInvoker* invoker);

    void Start();
    void Stop();

private:
    void Poll();
    
    typedef TChunkHolderServiceProxy TProxy;

    TChunkHolderConfig::TPtr Config;
    TIntrusivePtr<TBlockStore> BlockStore;
    NRpc::TChannelCache* ChannelCache;
    TIntrusivePtr<TPeriodicInvoker> PeriodicInvoker;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
