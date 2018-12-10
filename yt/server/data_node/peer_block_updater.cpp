#include "peer_block_updater.h"
#include "private.h"
#include "chunk_block_manager.h"
#include "config.h"
#include "master_connector.h"

#include <yt/server/cell_node/bootstrap.h>

#include <yt/ytlib/chunk_client/data_node_service_proxy.h>

#include <yt/ytlib/api/native/client.h>
#include <yt/ytlib/api/native/connection.h>

#include <yt/core/concurrency/periodic_executor.h>

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
        bootstrap->GetControlInvoker(),
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
    LOG_INFO("Updating peer blocks");

    auto expirationTime = GetPeerUpdateExpirationTime().ToDeadLine();
    auto localDescriptor = Bootstrap_
        ->GetMasterConnector()
        ->GetLocalDescriptor();
    const auto& channelFactory = Bootstrap_
        ->GetMasterClient()
        ->GetNativeConnection()
        ->GetChannelFactory();

    typedef TDataNodeServiceProxy TProxy;
    THashMap<TString, TProxy::TReqUpdatePeerPtr> requests;

    auto blocks = Bootstrap_->GetChunkBlockManager()->GetAllBlocks();
    for (const auto& block : blocks) {
        if (block->Source()) {
            const auto& sourceAddress = block->Source()->GetAddressOrThrow(Bootstrap_->GetLocalNetworks());
            TProxy::TReqUpdatePeerPtr request;
            auto it = requests.find(sourceAddress);
            if (it != requests.end()) {
                request = it->second;
            } else {
                auto channel = channelFactory->CreateChannel(sourceAddress);
                TProxy proxy(channel);
                request = proxy.UpdatePeer();
                request->SetMultiplexingBand(NRpc::EMultiplexingBand::Heavy);
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

} // namespace NYT::NDataNode
