#include "block_peer_updater.h"
#include "private.h"
#include "chunk_block_manager.h"
#include "config.h"

#include <yt/yt/server/node/cluster_node/bootstrap.h>
#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/master_connector.h>

#include <yt/yt/ytlib/chunk_client/data_node_service_proxy.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/core/rpc/dispatcher.h>

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

namespace NYT::NDataNode {

using namespace NClusterNode;
using namespace NChunkClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = P2PLogger;

////////////////////////////////////////////////////////////////////////////////

TBlockPeerUpdater::TBlockPeerUpdater(TBootstrap* bootstrap)
    : Bootstrap_(bootstrap)
    , Config_(Bootstrap_->GetConfig()->DataNode)
    , PeriodicExecutor_(New<TPeriodicExecutor>(
        Bootstrap_->GetStorageHeavyInvoker(),
        BIND(&TBlockPeerUpdater::Update, MakeWeak(this)),
        Config_->PeerUpdatePeriod))
{ }

void TBlockPeerUpdater::Start()
{
    PeriodicExecutor_->Start();
}

void TBlockPeerUpdater::Stop()
{
    PeriodicExecutor_->Stop();
}

TDuration TBlockPeerUpdater::GetPeerUpdateExpirationTime() const
{
    return Config_->PeerUpdateExpirationTime;
}

void TBlockPeerUpdater::Update()
{
    VERIFY_THREAD_AFFINITY_ANY();

    YT_LOG_INFO("Updating peer blocks");

    auto blocks = Bootstrap_->GetChunkBlockManager()->GetAllBlocksWithSource();

    YT_LOG_INFO("Cached blocks snapshot captured (BlockCount: %v)",
        blocks.size());

    auto expirationDeadline = GetPeerUpdateExpirationTime().ToDeadLine();
    auto localDescriptor = Bootstrap_
        ->GetClusterNodeMasterConnector()
        ->GetLocalDescriptor();
    const auto& channelFactory = Bootstrap_
        ->GetMasterClient()
        ->GetNativeConnection()
        ->GetChannelFactory();

    THashMap<TString, TDataNodeServiceProxy::TReqUpdatePeerPtr> requests;
    for (const auto& block : blocks) {
        YT_VERIFY(block->Source());
        const auto& sourceAddress = block->Source()->GetAddressOrThrow(Bootstrap_->GetLocalNetworks());

        auto it = requests.find(sourceAddress);
        if (it == requests.end()) {
            auto channel = channelFactory->CreateChannel(sourceAddress);
            TDataNodeServiceProxy proxy(channel);
            it = requests.emplace(sourceAddress, proxy.UpdatePeer()).first;
        }

        const auto& request = it->second;
        request->SetMultiplexingBand(NRpc::EMultiplexingBand::Heavy);
        request->set_peer_node_id(Bootstrap_->GetClusterNodeMasterConnector()->GetNodeId());
        request->set_peer_expiration_deadline(ToProto<ui64>(expirationDeadline));
        auto* protoBlockId = request->add_block_ids();
        const auto& blockId = block->GetKey();
        ToProto(protoBlockId->mutable_chunk_id(), blockId.ChunkId);
        protoBlockId->set_block_index(blockId.BlockIndex);
    }

    for (const auto& [address, request] : requests) {
        YT_LOG_DEBUG("Sending peer block update request (Address: %v, ExpirationDeadline: %v)",
            address,
            expirationDeadline);
        request->Invoke();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
