#include "stdafx.h"
#include "peer_block_updater.h"
#include "private.h"
#include "block_store.h"
#include "config.h"
#include "master_connector.h"

#include <core/concurrency/periodic_executor.h>

#include <ytlib/chunk_client/data_node_service_proxy.h>

#include <server/cell_node/bootstrap.h>

namespace NYT {
namespace NDataNode {

using namespace NCellNode;
using namespace NChunkClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = DataNodeLogger;

////////////////////////////////////////////////////////////////////////////////

TPeerBlockUpdater::TPeerBlockUpdater(
    TDataNodeConfigPtr config,
    TBootstrap* bootstrap)
    : Config(config)
    , Bootstrap(bootstrap)
{
    PeriodicExecutor = New<TPeriodicExecutor>(
        bootstrap->GetControlInvoker(),
        BIND(&TPeerBlockUpdater::Update, MakeWeak(this)),
        Config->PeerUpdatePeriod);
}

void TPeerBlockUpdater::Start()
{
    PeriodicExecutor->Start();
}

void TPeerBlockUpdater::Stop()
{
    PeriodicExecutor->Stop();
}

void TPeerBlockUpdater::Update()
{
    LOG_INFO("Updating peer blocks");

    auto expirationTime = Config->PeerUpdateExpirationTimeout.ToDeadLine();
    auto localDescriptor = Bootstrap->GetLocalDescriptor();

    typedef TDataNodeServiceProxy TProxy;

    yhash_map<Stroka, TProxy::TReqUpdatePeerPtr> requests;

    auto blocks = Bootstrap->GetBlockStore()->GetAllBlocks();
    for (auto block : blocks) {
        if (block->Source()) {
            const auto& sourceAddress = block->Source()->GetInterconnectAddress();
            TProxy::TReqUpdatePeerPtr request;
            auto it = requests.find(sourceAddress);
            if (it != requests.end()) {
                request = it->second;
            } else {
                TProxy proxy(ChannelFactory->CreateChannel(sourceAddress));
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

    for (const auto& pair : requests) {
        LOG_DEBUG("Sending peer block update request (Address: %v, ExpirationTime: %v)",
            pair.first,
            expirationTime);
        pair.second->Invoke();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
