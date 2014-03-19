#include "stdafx.h"
#include "partition_balancer.h"
#include "config.h"
#include "tablet_slot.h"
#include "tablet_cell_controller.h"
#include "tablet_manager.h"
#include "tablet.h"
#include "partition.h"
#include "store.h"
#include "chunk_store.h"
#include "private.h"

#include <core/concurrency/fiber.h>

#include <core/logging/tagged_logger.h>

#include <ytlib/tablet_client/config.h>

#include <ytlib/new_table_client/unversioned_row.h>
#include <ytlib/new_table_client/samples_fetcher.h>

#include <ytlib/node_tracker_client/node_directory.h>

#include <ytlib/chunk_client/chunk_spec.h>
#include <ytlib/chunk_client/chunk_service_proxy.h>

#include <server/hydra/hydra_manager.h>
#include <server/hydra/mutation.h>

#include <server/tablet_node/tablet_manager.pb.h>

#include <server/cell_node/bootstrap.h>

namespace NYT {
namespace NTabletNode {

using namespace NConcurrency;
using namespace NHydra;
using namespace NVersionedTableClient;
using namespace NNodeTrackerClient;
using namespace NChunkClient;
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

            partition->SetState(EPartitionState::Splitting);

            RunSplit(partition);
        }
        
        if (dataSize + tablet->GetEden()->GetTotalDataSize() < config->MinPartitionDataSize && partitionCount > 1) {
            int firstPartitionIndex = partition->GetIndex();
            int lastPartitionIndex = firstPartitionIndex + 1;

            if (lastPartitionIndex == partitionCount) {
                --firstPartitionIndex;
                --lastPartitionIndex;
            }

            for (int index = firstPartitionIndex; index <= lastPartitionIndex; ++index) {
                if (tablet->Partitions()[index]->GetState() != EPartitionState::None)
                    return;
            }

            for (int index = firstPartitionIndex; index <= lastPartitionIndex; ++index) {
                tablet->Partitions()[index]->SetState(EPartitionState::Splitting);
            }

            RunMerge(partition, firstPartitionIndex, lastPartitionIndex);
        }
    }


    void RunSplit(TPartition* partition)
    {
        BIND(&TPartitionBalancer::DoRunSplit, MakeStrong(this))
            .AsyncVia(partition->GetTablet()->GetEpochAutomatonInvoker(EAutomatonThreadQueue::Write))
            .Run(partition);
    }

    void DoRunSplit(TPartition* partition)
    {
        auto Logger = BuildLogger(partition);

        auto* tablet = partition->GetTablet();
        auto slot = tablet->GetSlot();
        auto hydraManager = slot->GetHydraManager();

        LOG_INFO("Partition is eligible for split");

        try {
            auto nodeDirectory = New<TNodeDirectory>();

            auto fetcher = New<TSamplesFetcher>(
                Config_->SamplesFetcher,
                Config_->SamplesFetcher->MaxSampleCount,
                tablet->KeyColumns(),
                nodeDirectory,
                GetCurrentInvoker(),
                Logger);

            {
                LOG_INFO("Locating store chunks");

                TChunkServiceProxy proxy(Bootstrap_->GetMasterChannel());
                auto req = proxy.LocateChunks();

                for (auto store : partition->Stores()) {
                    YCHECK(store->GetState() == EStoreState::Persistent);
                    auto chunkId = store->GetId();
                    ToProto(req->add_chunk_ids(), chunkId);
                }

                auto rsp = WaitFor(req->Invoke());
                THROW_ERROR_EXCEPTION_IF_FAILED(*rsp);

                LOG_INFO("Store chunks located");

                nodeDirectory->MergeFrom(rsp->node_directory());

                for (const auto& chunkInfo : rsp->chunks()) {
                    auto chunkId = FromProto<TChunkId>(chunkInfo.chunk_id());
                    auto store = tablet->GetStore(chunkId);
                    auto* chunkStore = dynamic_cast<TChunkStore*>(store.Get());

                    auto chunkSpec = New<TRefCountedChunkSpec>();
                    chunkSpec->mutable_chunk_id()->CopyFrom(chunkInfo.chunk_id());
                    chunkSpec->mutable_replicas()->MergeFrom(chunkInfo.replicas());
                    chunkSpec->mutable_chunk_meta()->CopyFrom(chunkStore->GetChunkMeta());
                    fetcher->AddChunk(chunkSpec);
                }
            }

            {
                auto result = WaitFor(fetcher->Fetch());
                THROW_ERROR_EXCEPTION_IF_FAILED(result);
            }

            auto samples = fetcher->GetSamples();
            int sampleCount = static_cast<int>(samples.size());
            if (sampleCount < Config_->SamplesFetcher->MinSampleCount) {
                THROW_ERROR_EXCEPTION("Too few samples fetched: %d < %d",
                    sampleCount,
                    Config_->SamplesFetcher->MinSampleCount);
            }

            std::sort(samples.begin(), samples.end());

            const auto& splitKey = samples[sampleCount / 2];
            if (splitKey == partition->GetPivotKey() || splitKey == partition->GetNextPivotKey()) {
                THROW_ERROR_EXCEPTION("No valid split key can be obtained from samples");
            }

            LOG_INFO("Split key is %s", ~ToString(splitKey));

            TReqSplitPartition request;
            ToProto(request.mutable_tablet_id(), tablet->GetId());
            ToProto(request.add_pivot_keys(), partition->GetPivotKey());
            ToProto(request.add_pivot_keys(), splitKey);
            CreateMutation(hydraManager, request)
                ->Commit();
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Partitioning aborted");
            partition->SetState(EPartitionState::None);
        }
    }


    void RunMerge(
        TPartition* partition,
        int firstPartitionIndex,
        int lastPartitionIndex)
    {
        auto Logger = BuildLogger(partition);

        LOG_INFO("Partition is eligible for merge");

        auto* tablet = partition->GetTablet();
        auto slot = tablet->GetSlot();
        auto hydraManager = slot->GetHydraManager();

        TReqMergePartitions request;
        ToProto(request.mutable_tablet_id(), tablet->GetId());
        ToProto(request.mutable_pivot_key(), tablet->Partitions()[firstPartitionIndex]->GetPivotKey());
        request.set_partition_count(lastPartitionIndex - firstPartitionIndex + 1);
        CreateMutation(hydraManager, request)
            ->Commit();
    }


    static NLog::TTaggedLogger BuildLogger(TPartition* partition)
    {
        NLog::TTaggedLogger logger(TabletNodeLogger);
        logger.AddTag(Sprintf("TabletId: %s, PartitionKeys: %s .. %s",
            ~ToString(partition->GetTablet()->GetId()),
            ~ToString(partition->GetPivotKey()),
            ~ToString(partition->GetNextPivotKey())));
        return logger;
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
