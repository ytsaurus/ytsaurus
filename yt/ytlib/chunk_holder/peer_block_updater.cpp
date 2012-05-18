#include "stdafx.h"
#include "peer_block_updater.h"
#include "common.h"
#include "block_store.h"
#include "chunk_holder_service_proxy.h"
#include "bootstrap.h"
#include "config.h"

#include <ytlib/rpc/channel_cache.h>
#include <ytlib/cell_node/bootstrap.h>

namespace NYT {
namespace NChunkHolder {

using namespace NCellNode;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkHolderLogger;
static NRpc::TChannelCache ChannelCache;

////////////////////////////////////////////////////////////////////////////////

TPeerBlockUpdater::TPeerBlockUpdater(
    TChunkHolderConfigPtr config,
    TBootstrap* bootstrap)
    : Config(config)
    , Bootstrap(bootstrap)
{
    PeriodicInvoker = New<TPeriodicInvoker>(
        bootstrap->GetControlInvoker(),
        BIND(&TPeerBlockUpdater::Update, MakeWeak(this)),
        Config->PeerUpdatePeriod);
}

void TPeerBlockUpdater::Start()
{
    PeriodicInvoker->Start();
}

void TPeerBlockUpdater::Stop()
{
    PeriodicInvoker->Stop();
}

void TPeerBlockUpdater::Update()
{
    PeriodicInvoker->ScheduleNext();

    LOG_INFO("Updating peer blocks");

    auto expirationTime = Config->PeerUpdateExpirationTimeout.ToDeadLine();

    yhash_map<Stroka, TProxy::TReqUpdatePeerPtr> requests;

    auto blocks = Bootstrap->GetBlockStore()->GetAllBlocks();
    auto peerAddress = Bootstrap->GetPeerAddress();
    FOREACH (auto block, blocks) {
        const auto& source = block->Source();
        if (!source.Empty()) {
            TProxy::TReqUpdatePeerPtr request;
            auto it = requests.find(source);
            if (it != requests.end()) {
                request = it->second;
            } else {
                TProxy proxy(~ChannelCache.GetChannel(source));
                request = proxy.UpdatePeer();
                request->set_peer_address(peerAddress);
                request->set_peer_expiration_time(expirationTime.GetValue());
                requests.insert(MakePair(source, request));
            }
            auto* block_id = request->add_block_ids();
            const auto& blockId = block->GetKey();
            *block_id->mutable_chunk_id() = blockId.ChunkId.ToProto();
            block_id->set_block_index(blockId.BlockIndex);
        }
    }

    FOREACH (const auto& pair, requests) {
        LOG_DEBUG("Sending peer block update request (Address: %s, ExpirationTime: %s)",
            ~pair.first,
            ~expirationTime.ToString());
        pair.second->Invoke();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
