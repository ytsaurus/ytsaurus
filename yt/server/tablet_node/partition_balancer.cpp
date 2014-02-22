#include "stdafx.h"
#include "partition_balancer.h"
#include "config.h"
#include "tablet_slot.h"
#include "tablet_cell_controller.h"
#include "tablet_manager.h"
#include "tablet.h"
#include "partition.h"
#include "store.h"
#include "private.h"

#include <ytlib/tablet_client/config.h>

#include <ytlib/new_table_client/unversioned_row.h>

#include <server/hydra/hydra_manager.h>
#include <server/hydra/mutation.h>

#include <server/tablet_node/tablet_manager.pb.h>

#include <server/cell_node/bootstrap.h>

namespace NYT {
namespace NTabletNode {

using namespace NHydra;
using namespace NVersionedTableClient;
using namespace NTabletNode::NProto;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = TabletNodeLogger;

////////////////////////////////////////////////////////////////////////////////

class TPartitionBalancer
    : public TRefCounted
{
public:
    TPartitionBalancer(
        TPartitionBalancerConfigPtr config,
        NCellNode::TBootstrap* bootstrap)
        : Config_(config)
        , Bootstrap_(bootstrap)
    { }

    void Start()
    {
        auto tabletCellController = Bootstrap_->GetTabletCellController();
        tabletCellController->SubscribeSlotScan(BIND(&TPartitionBalancer::ScanSlot, MakeStrong(this)));
    }

private:
    TPartitionBalancerConfigPtr Config_;
    NCellNode::TBootstrap* Bootstrap_;


    void ScanSlot(TTabletSlotPtr slot)
    {
        if (slot->GetAutomatonState() != EPeerState::Leading)
            return;

        auto tabletManager = slot->GetTabletManager();
        auto tablets = tabletManager->Tablets().GetValues();
        for (auto* tablet : tablets) {
            ScanTablet(slot, tablet);
        }
    }

    void ScanTablet(TTabletSlotPtr slot, TTablet* tablet)
    {
        for (const auto& partition : tablet->Partitions()) {
            ScanPartition(slot, partition.get());
        }
    }

    void ScanPartition(TTabletSlotPtr slot, TPartition* partition)
    {
        i64 dataSize = partition->GetTotalDataSize();
        
        auto* tablet = partition->GetTablet();
        int partitionCount = static_cast<int>(tablet->Partitions().size());

        const auto& config = tablet->GetConfig();

        if (dataSize >  config->MaxPartitionDataSize && partitionCount < config->MaxPartitionCount) {
            if (partition->GetState() != EPartitionState::None)
                return;

            auto splitKey = ChoosePartitionSplitKey(partition);
            if (!splitKey) 
                return;

            partition->SetState(EPartitionState::Splitting);

            auto hydraManager = slot->GetHydraManager();

            TReqSplitPartition request;
            ToProto(request.mutable_tablet_id(), tablet->GetId());
            ToProto(request.add_pivot_keys(), partition->GetPivotKey());
            ToProto(request.add_pivot_keys(), splitKey);
            CreateMutation(hydraManager, request)
                ->Commit();
        }
        
        if (dataSize < config->MinPartitionDataSize && partitionCount > 1) {
            // TODO(babenko)
            //int firstPartitionIndex = partition->GetIndex();
            //int lastPartitionIndex = firstPartitionIndex + 1;

            //if (lastPartitionIndex == partitionCount) {
            //    --firstPartitionIndex;
            //    --lastPartitionIndex;
            //}

            //for (int index = firstPartitionIndex; index <= lastPartitionIndex; ++index) {
            //    if (tablet->Partitions()[index]->GetState() != EPartitionState::None)
            //        return;
            //}

            //for (int index = firstPartitionIndex; index <= lastPartitionIndex; ++index) {
            //    tablet->Partitions()[index]->SetState(EPartitionState::Splitting);
            //}

            //auto hydraManager = slot->GetHydraManager();

            //TReqMergePartitions request;
            //ToProto(request.mutable_tablet_id(), tablet->GetId());
            //ToProto(request.mutable_pivot_key(), tablet->Partitions()[firstPartitionIndex]->GetPivotKey());
            //request.set_partition_count(lastPartitionIndex - firstPartitionIndex + 1);
            //CreateMutation(hydraManager, request)
            //    ->Commit();
        }
    }


    TOwningKey ChoosePartitionSplitKey(TPartition* partition)
    {
        // TODO(babenko): rewrite using sample fetcher
        auto isValidKey = [&] (const TOwningKey& key) {
            return key > partition->GetPivotKey() && key < partition->GetNextPivotKey();
        };

        std::vector<TOwningKey> keys;
        for (const auto& store : partition->Stores()) {
            if (isValidKey(store->GetMinKey())) {
                keys.push_back(store->GetMinKey());
            }
            if (isValidKey(store->GetMaxKey())) {
                keys.push_back(store->GetMaxKey());
            }
        }
        
        if (keys.empty()) {
            return TOwningKey();
        }

        std::sort(keys.begin(), keys.end());
        return keys[keys.size() / 2];
    }

};

void StartPartitionBalancer(
    TPartitionBalancerConfigPtr config,
    NCellNode::TBootstrap* bootstrap)
{
    New<TPartitionBalancer>(config, bootstrap)->Start();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
