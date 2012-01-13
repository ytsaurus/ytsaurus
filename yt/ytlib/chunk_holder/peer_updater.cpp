#include "stdafx.h"
#include "peer_updater.h"

#include "block_store.h"
#include "chunk_holder_service_proxy.h"

#include <ytlib/rpc/channel_cache.h>

#include <misc/periodic_invoker.h>

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkHolderLogger;

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
    LOG_INFO("Updating peers");

    auto expirationTime = Config->PeerUpdateExpirationTimeout.ToDeadLine();

    yhash_map<Stroka, TProxy::TReqUpdatePeer::TPtr> requests;

    auto blocks = BlockStore->GetAllBlocks();
    FOREACH (auto block, blocks) {
        const auto& source = block->Source();
        if (!source.Empty()) {
            TProxy::TReqUpdatePeer::TPtr request;
            auto it = requests.find(source);
            if (it != requests.end()) {
                request = it->Second();
            } else {
                TProxy proxy(~ChannelCache->GetChannel(source));
                request = proxy.UpdatePeer();
                request->set_peer_address(Config->PeerAddress);
                request->set_peer_expiration_time(expirationTime.GetValue());
                requests.insert(MakePair(source, request));
            }
            auto* block_id = request->add_block_ids();
            const auto& blockId = block->GetKey();
            block_id->set_chunk_id(blockId.ChunkId.ToProto());
            block_id->set_block_index(blockId.BlockIndex);
        }
    }

    FOREACH (const auto& pair, requests) {
        LOG_DEBUG("Requesting peer update (Address: %s, ExpirationTime: %s)",
            ~pair.First(),
            ~expirationTime.ToString());
        pair.Second()->Invoke();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
