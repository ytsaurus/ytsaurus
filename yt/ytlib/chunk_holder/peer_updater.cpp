#include "stdafx.h"
#include "peer_updater.h"

#include "block_store.h"
#include "chunk_holder_service_proxy.h"

#include <ytlib/rpc/channel_cache.h>

#include <misc/periodic_invoker.h>

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

TPeerUpdater::TPeerUpdater(
    TChunkHolderConfig* config,
    TBlockStore* blockStore,
    NRpc::TChannelCache* channelCache,
    IInvoker* invoker)
    : Config(config)
    , BlockStore(blockStore)
    , ChannelCache(channelCache)
{
    PeriodicInvoker = New<TPeriodicInvoker>(
        FromMethod(&TPeerUpdater::Poll, TPtr(this))
        ->Via(invoker),
        Config->PeerUpdatePeriod);
}

void TPeerUpdater::Start()
{
    PeriodicInvoker->Start();
}

void TPeerUpdater::Stop()
{
    PeriodicInvoker->Stop();
}

void TPeerUpdater::Poll()
{
    auto blocks = BlockStore->GetAllBlocks();
    FOREACH (auto block, blocks) {
        const auto& source = block->Source();
        if (!source.Empty()) {
            TProxy proxy(~ChannelCache->GetChannel(source));
            auto request = proxy.UpdatePeer();
            request->set_peer_address(Config->PeerAddress);
            request->set_peer_expiration_time(
                Config->PeerUpdateExpirationTimeout.ToDeadLine().GetValue());
            request->Invoke();
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
