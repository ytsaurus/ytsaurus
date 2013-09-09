#include "stdafx.h"
#include "peer_block_updater.h"
#include "private.h"
#include "block_store.h"
#include "config.h"
#include "master_connector.h"

#include <core/rpc/channel_cache.h>

#include <ytlib/chunk_client/data_node_service_proxy.h>

#include <server/cell_node/bootstrap.h>

namespace NYT {
namespace NDataNode {

using namespace NCellNode;
using namespace NChunkClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = DataNodeLogger;

////////////////////////////////////////////////////////////////////////////////

TPeerBlockUpdater::TPeerBlockUpdater(
    TDataNodeConfigPtr config,
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
    LOG_INFO("Updating peer blocks");

    auto expirationTime = Config->PeerUpdateExpirationTimeout.ToDeadLine();
    auto localDescriptor = Bootstrap->GetLocalDescriptor();

    yhash_map<Stroka, TProxy::TReqUpdatePeerPtr> requests;

    auto blocks = Bootstrap->GetBlockStore()->GetAllBlocks();
    FOREACH (auto block, blocks) {
        if (block->Source()) {
            const auto& sourceAddress = block->Source()->Address;
            TProxy::TReqUpdatePeerPtr request;
            auto it = requests.find(sourceAddress);
            if (it != requests.end()) {
                request = it->second;
            } else {
                TProxy proxy(ChannelCache.GetChannel(sourceAddress));
                request = proxy.UpdatePeer();
                ToProto(request->mutable_peer_descriptor(), localDescriptor);
                request->set_peer_expiration_time(expirationTime.GetValue());
                requests.insert(std::make_pair(sourceAddress, request));
            }
            auto* block_id = request->add_block_ids();
            const auto& blockId = block->GetKey();
            ToProto(block_id->mutable_chunk_id(), blockId.ChunkId);
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

} // namespace NDataNode
} // namespace NYT
