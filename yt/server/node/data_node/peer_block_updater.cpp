#include "peer_block_updater.h"
#include "private.h"
#include "chunk_block_manager.h"
#include "config.h"
#include "master_connector.h"

#include <yt/server/node/cell_node/bootstrap.h>

#include <yt/ytlib/chunk_client/data_node_service_proxy.h>

#include <yt/ytlib/api/native/client.h>
#include <yt/ytlib/api/native/connection.h>

#include <yt/core/rpc/dispatcher.h>

#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/thread_affinity.h>

namespace NYT::NDataNode {

using namespace NCellNode;
using namespace NChunkClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = P2PLogger;

////////////////////////////////////////////////////////////////////////////////

TPeerBlockUpdater::TPeerBlockUpdater(
    TDataNodeConfigPtr config,
    TBootstrap* bootstrap)
    : Config_(config)
    , Bootstrap_(bootstrap)
    , PeriodicExecutor_(New<TPeriodicExecutor>(
        NRpc::TDispatcher::Get()->GetHeavyInvoker(),
        BIND(&TPeerBlockUpdater::Update, MakeWeak(this)),
        Config_->PeerUpdatePeriod))
{ }

void TPeerBlockUpdater::Start()
{
    PeriodicExecutor_->Start();
}

void TPeerBlockUpdater::Stop()
{
    PeriodicExecutor_->Stop();
}

TDuration TPeerBlockUpdater::GetPeerUpdateExpirationTime() const
{
    return Config_->PeerUpdateExpirationTime;
}

void TPeerBlockUpdater::Update()
{
    VERIFY_THREAD_AFFINITY_ANY();

    YT_LOG_INFO("Updating peer blocks");

    auto expirationTime = GetPeerUpdateExpirationTime().ToDeadLine();
    auto localDescriptor = Bootstrap_
        ->GetMasterConnector()
        ->GetLocalDescriptor();
    const auto& channelFactory = Bootstrap_
        ->GetMasterClient()
        ->GetNativeConnection()
        ->GetChannelFactory();

    THashMap<TString, TDataNodeServiceProxy::TReqUpdatePeerPtr> requests;

    auto blocks = Bootstrap_->GetChunkBlockManager()->GetAllBlocks();
    for (const auto& block : blocks) {
        if (block->Source()) {
            const auto& sourceAddress = block->Source()->GetAddressOrThrow(Bootstrap_->GetLocalNetworks());
            auto it = requests.find(sourceAddress);
            if (it == requests.end()) {
                auto channel = channelFactory->CreateChannel(sourceAddress);
                TDataNodeServiceProxy proxy(channel);
                it = requests.emplace(sourceAddress, proxy.UpdatePeer()).first;
            }

            const auto& request = it->second;
            request->SetMultiplexingBand(NRpc::EMultiplexingBand::Heavy);
            request->set_peer_node_id(Bootstrap_->GetMasterConnector()->GetNodeId());
            // COMPAT(psushin).
            ToProto(request->mutable_peer_descriptor(), localDescriptor);
            request->set_peer_expiration_time(expirationTime.GetValue());
            auto* protoBlockId = request->add_block_ids();
            const auto& blockId = block->GetKey();
            ToProto(protoBlockId->mutable_chunk_id(), blockId.ChunkId);
            protoBlockId->set_block_index(blockId.BlockIndex);
        }
    }

    for (const auto& [address, request] : requests) {
        YT_LOG_DEBUG("Sending peer block update request (Address: %v, ExpirationTime: %v)",
            address,
            expirationTime);
        request->Invoke();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
