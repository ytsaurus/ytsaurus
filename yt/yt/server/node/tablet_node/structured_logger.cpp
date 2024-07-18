#include "structured_logger.h"

#include "bootstrap.h"
#include "hunk_chunk.h"
#include "partition.h"
#include "private.h"
#include "slot_manager.h"
#include "sorted_chunk_store.h"
#include "store.h"
#include "store_detail.h"
#include "tablet.h"
#include "tablet_manager.h"
#include "tablet_slot.h"

#include <yt/yt/server/node/cluster_node/bootstrap.h>
#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/core/logging/fluent_log.h>

#include <yt/yt/core/utilex/random.h>

namespace NYT::NTabletNode {

using namespace NLogging;
using namespace NYTree;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NClusterNode;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

//! Sent in every FullHeartbeat event.
static constexpr int ProtocolVersion = 1;

////////////////////////////////////////////////////////////////////////////////

class TStructuredLogger
    : public IStructuredLogger
{
public:
    explicit TStructuredLogger(IBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
        , Logger_(LsmLogger())
    {
        const auto& dynamicConfigManager = Bootstrap_->GetDynamicConfigManager();
        Config_ = dynamicConfigManager->GetConfig()->TabletNode;
        dynamicConfigManager->SubscribeConfigChanged(BIND(
            &TStructuredLogger::OnDynamicConfigChanged,
            MakeWeak(this)));

        const auto& slotManager = Bootstrap_->GetSlotManager();
        slotManager->SubscribeScanSlot(BIND(
            &TStructuredLogger::OnScanSlot,
            MakeWeak(this)));
    }

    TOneShotFluentLogEvent LogEvent(TStringBuf eventType) override
    {
        return LogStructuredEventFluently(ELogEntryType::Event)
            .Item("event_type").Value(eventType);
    }

    IPerTabletStructuredLoggerPtr CreateLogger(TTablet* tablet) override;

    void OnHeartbeatRequest(const ITabletManagerPtr& tabletManager, bool initial) override
    {
        if (!Enabled_) {
            return;
        }

        TDuration fullHeartbeatPeriod;
        TDuration incrementalHeartbeatPeriod;

        {
            auto guard = ReaderGuard(SpinLock_);
            fullHeartbeatPeriod = Config_->FullStructuredTabletHeartbeatPeriod;
            incrementalHeartbeatPeriod = Config_->IncrementalStructuredTabletHeartbeatPeriod;
        }

        auto now = TInstant::Now();

        for (auto [tabletId, tablet] : tabletManager->Tablets()) {
            if (initial) {
                tablet->GetStructuredLogger()->OnFullHeartbeat();
                tablet->SetLastFullStructuredHeartbeatTime(
                    now - RandomDuration(fullHeartbeatPeriod));
                tablet->SetLastIncrementalStructuredHeartbeatTime(
                    now - RandomDuration(incrementalHeartbeatPeriod));
                break;
            }

            auto lastFull = tablet->GetLastFullStructuredHeartbeatTime();
            auto lastIncremental = tablet->GetLastIncrementalStructuredHeartbeatTime();
            lastIncremental = std::max(lastIncremental, lastFull);

            if (now - lastFull > fullHeartbeatPeriod) {
                tablet->GetStructuredLogger()->OnFullHeartbeat();
            } else if (now - lastIncremental > incrementalHeartbeatPeriod) {
                tablet->GetStructuredLogger()->OnIncrementalHeartbeat();
            }
        }
    }

    TOneShotFluentLogEvent LogStructuredEventFluently(ELogEntryType entryType)
    {
        auto sequenceNumber = SequenceNumber_.fetch_add(1, std::memory_order::relaxed);
        auto entry = Enabled_
            ? NLogging::LogStructuredEventFluently(Logger_, ELogLevel::Info)
            : NLogging::LogStructuredEventFluentlyToNowhere();
        return entry
            .Item("sequence_number").Value(sequenceNumber)
            .Item("entry_type").Value(entryType);
    }

private:
    IBootstrap* const Bootstrap_;
    const NLogging::TLogger Logger_;

    // TODO(ifsmirnov): MPSC queue collector thread that handles reordering and throttling.
    std::atomic<i64> SequenceNumber_ = 0;
    std::atomic<bool> Enabled_ = true;

    TTabletNodeDynamicConfigPtr Config_;
    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, SpinLock_);

    void OnDynamicConfigChanged(
        const TClusterNodeDynamicConfigPtr& /*oldConfig*/,
        const TClusterNodeDynamicConfigPtr& newConfig)
    {
        Enabled_.store(newConfig->TabletNode->EnableStructuredLogger);

        auto guard = WriterGuard(SpinLock_);
        Config_ = newConfig->TabletNode;
    }

    void OnScanSlot(const ITabletSlotPtr& slot)
    {
        if (!Enabled_) {
            return;
        }

        OnHeartbeatRequest(slot->GetTabletManager(), false);
    }
};

DECLARE_REFCOUNTED_CLASS(TStructuredLogger)
DEFINE_REFCOUNTED_TYPE(TStructuredLogger)

////////////////////////////////////////////////////////////////////////////////

class TPerTabletStructuredLogger
    : public IPerTabletStructuredLogger
{
public:
    TPerTabletStructuredLogger(TStructuredLoggerPtr logger, TTablet* tablet)
        : Logger_(std::move(logger))
        , Tablet_(tablet)
        , TabletId_(Tablet_->GetId())
        , Enabled_(Tablet_->GetSettings().MountConfig->EnableStructuredLogger)
    { }

    // Only for tests.
    explicit TPerTabletStructuredLogger(TTablet* tablet)
        : Tablet_(tablet)
        , TabletId_(tablet->GetId())
        , Enabled_(false)
    { }

    TTabletId GetTabletId() const override
    {
        return TabletId_;
    }

    void SetEnabled(bool enabled) override
    {
        YT_VERIFY(Logger_);
        Enabled_ = enabled;
    }

    TOneShotFluentLogEvent LogEvent(TStringBuf eventType) override
    {
        if (!Enabled_) {
            return LogStructuredEventFluentlyToNowhere();
        }

        return Logger_->LogEvent(eventType)
            .Item("tablet_id").Value(TabletId_);
    }

    void OnFullHeartbeat() override
    {
        if (!Enabled_) {
            return;
        }

        Logger_->LogStructuredEventFluently(ELogEntryType::FullHeartbeat)
            .Item("protocol_version").Value(ProtocolVersion)
            .Item("tablet_id").Value(Tablet_->GetId())
            .Item("table_id").Value(Tablet_->GetTableId())
            .Item("table_path").Value(Tablet_->GetTablePath())
            .Item("state").Value(Tablet_->GetState())
            .Item("mount_revision").Value(Tablet_->GetMountRevision())
            .DoIf(Tablet_->IsPhysicallySorted(), [&] (auto fluent) {
                fluent
                    .Item("pivot_key").Value(Tablet_->GetPivotKey())
                    .Item("next_pivot_key").Value(Tablet_->GetNextPivotKey())
                    .Item("eden").DoMap(BIND(
                        &TPerTabletStructuredLogger::OnPartitionFullHeartbeat,
                        Unretained(this),
                        Tablet_->GetEden()))
                    .Item("partitions").DoListFor(
                        Tablet_->PartitionList(),
                        [&] (auto fluent, const std::unique_ptr<TPartition>& partition) {
                            fluent
                                .Item()
                                .DoMap(BIND(
                                    &TPerTabletStructuredLogger::OnPartitionFullHeartbeat,
                                    Unretained(this),
                                    partition.get()));
                    });

            })
            .Item("hunk_chunks").DoMapFor(
                Tablet_->HunkChunkMap(),
                [&] (auto fluent, const auto& pair) {
                    const auto& [chunkId, hunkChunk] = pair;
                    fluent
                        .Item(ToString(chunkId))
                        .DoMap(BIND(
                            &TPerTabletStructuredLogger::OnHunkChunkFullHeartbeat,
                            Unretained(this),
                            hunkChunk));
            })
            .Item("dynamic_store_id_pool").List(Tablet_->DynamicStoreIdPool());

        Tablet_->SetLastFullStructuredHeartbeatTime(TInstant::Now());
    }

    void OnIncrementalHeartbeat() override
    {
        if (!Enabled_) {
            return;
        }

        std::vector<IDynamicStorePtr> dynamicStores;

        if (Tablet_->GetPhysicalSchema()->IsSorted()) {
            for (const auto& store : Tablet_->GetEden()->Stores()) {
                if (store->IsDynamic()) {
                    dynamicStores.push_back(store->AsDynamic());
                }
            }
        } else {
            for (const auto& [_, store] : Tablet_->StoreIdMap()) {
                if (store->IsDynamic()) {
                    dynamicStores.push_back(store->AsDynamic());
                }
            }
        }

        Logger_->LogStructuredEventFluently(ELogEntryType::IncrementalHeartbeat)
            .Item("tablet_id").Value(Tablet_->GetId())
            .Item("stores").DoMapFor(
                dynamicStores,
                [&] (auto fluent, const IDynamicStorePtr& store) {
                    fluent
                        .Item(ToString(store->GetId()))
                        .DoMap(BIND(
                            &TPerTabletStructuredLogger::OnDynamicStoreIncrementalHeartbeat,
                            Unretained(this),
                            store));
                });

        Tablet_->SetLastIncrementalStructuredHeartbeatTime(TInstant::Now());
    }

    void OnStoreRotated(
        const IDynamicStorePtr& previousStore,
        const IDynamicStorePtr& newStore) override
    {
        LogEvent("rotate_store")
            .Item("previous_store_id").Value(GetObjectId(previousStore))
            .Item("new_store_id").Value(GetObjectId(newStore))
            .DoIf(static_cast<bool>(newStore), [&] (auto fluent) {
                fluent
                    .Item("new_store").DoMap(BIND(
                        &TPerTabletStructuredLogger::OnDynamicStoreFullHeartbeat,
                        Unretained(this),
                        newStore));
            });
    }

    void OnBackingStoreSet(
        const IChunkStorePtr& store,
        const IDynamicStorePtr& backingStore) override
    {
        LogEvent("set_backing_store")
            .Item("store_id").Value(store->GetId())
            .Item("backing_store_id").Value(backingStore->GetId())
            .Item("backing_store").DoMap(BIND(
                &TPerTabletStructuredLogger::OnStoreFullHeartbeat,
                Unretained(this),
                backingStore));
    }

    void OnBackingStoreReleased(const IChunkStorePtr& store) override
    {
        LogEvent("release_backing_store")
            .Item("store_id").Value(store->GetId());
    }

    void OnTabletStoresUpdatePrepared(
        const std::vector<TStoreId>& addedStoreIds,
        const std::vector<TStoreId>& removedStoreIds,
        NTabletClient::ETabletStoresUpdateReason updateReason,
        TTransactionId transactionId) override
    {
        LogEvent("prepare_update_tablet_stores")
            .Item("added_store_ids").List(addedStoreIds)
            .Item("removed_store_ids").List(removedStoreIds)
            .Item("update_reason").Value(updateReason)
            .Item("transaction_id").Value(transactionId);
    }

    void OnTabletStoresUpdateCommitted(
        const std::vector<IStorePtr>& addedStores,
        const std::vector<TStoreId>& removedStoreIds,
        const std::vector<THunkChunkPtr>& addedHunkChunks,
        const std::vector<NChunkClient::TChunkId>& removedHunkChunkIds,
        ETabletStoresUpdateReason updateReason,
        TDynamicStoreId allocatedDynamicStoreId,
        TTransactionId transactionId) override
    {
        LogEvent("commit_update_tablet_stores")
            .Item("added_stores").DoMapFor(
                addedStores,
                [&] (auto fluent, const IStorePtr& store) {
                    fluent
                        .Item(ToString(store->GetId()))
                        .DoMap(BIND(
                            &TPerTabletStructuredLogger::OnStoreFullHeartbeat,
                            Unretained(this),
                            store));
                })
            .Item("removed_store_ids").List(removedStoreIds)
            .Item("added_hunk_chunks").DoMapFor(
                addedHunkChunks,
                [&] (auto fluent, const THunkChunkPtr& hunkChunk) {
                    fluent
                        .Item(ToString(hunkChunk->GetId()))
                        .DoMap(BIND(
                            &TPerTabletStructuredLogger::OnHunkChunkFullHeartbeat,
                            Unretained(this),
                            hunkChunk));
                })
            .Item("removed_hunk_chunk_ids").List(removedHunkChunkIds)
            .Item("update_reason").Value(updateReason)
            .DoIf(static_cast<bool>(allocatedDynamicStoreId), [&] (auto fluent) {
                fluent
                    .Item("allocated_dynamic_store_id").Value(allocatedDynamicStoreId);
                })
            .Item("transaction_id").Value(transactionId);
    }

    void OnTabletUnlocked(
        TRange<IStorePtr> stores,
        bool overwrite,
        TTransactionId transactionId) override
    {
        LogEvent("bulk_add_stores")
            .Item("added_stores").DoMapFor(
                stores,
                [&] (auto fluent, const IStorePtr& store) {
                    fluent
                        .Item(ToString(store->GetId()))
                        .DoMap(BIND(
                            &TPerTabletStructuredLogger::OnStoreFullHeartbeat,
                            Unretained(this),
                            store));
                })
            .Item("overwrite").Value(overwrite)
            .Item("transaction_id").Value(transactionId);
    }

    void OnTabletMounted() override
    {
        LogEvent("mount")
            .Item("mount_revision").Value(Tablet_->GetMountRevision());
    }

    void OnTabletUnmounted() override
    {
        LogEvent("unmount")
            .Item("mount_revision").Value(Tablet_->GetMountRevision());
    }

    void OnTabletFrozen() override
    {
        LogEvent("freeze");
    }

    void OnTabletUnfrozen(const std::vector<TDynamicStoreId>& dynamicStoreIds) override
    {
        LogEvent("unfreeze")
            .Item("allocated_dynamic_store_ids").Value(dynamicStoreIds);
    }

    void OnPartitionStateChanged(const TPartition* partition) override
    {
        LogEvent("set_partition_state")
            .Item("partition_id").Value(partition->GetId())
            .Item("state").Value(partition->GetState());
    }

    void OnStoreStateChanged(const IStorePtr& store) override
    {
        LogEvent("set_store_state")
            .Item("store_id").Value(store->GetId())
            .Item("state").Value(store->GetStoreState());
    }

    void OnHunkChunkStateChanged(const THunkChunkPtr& hunkChunk) override
    {
        LogEvent("set_hunk_chunk_state")
            .Item("chunk_id").Value(hunkChunk->GetId())
            .Item("state").Value(hunkChunk->GetState());
    }

    void OnStoreCompactionStateChanged(const IChunkStorePtr& store) override
    {
        LogEvent("set_store_compaction_state")
            .Item("store_id").Value(store->GetId())
            .Item("compaction_state").Value(store->GetCompactionState());
    }

    void OnStorePreloadStateChanged(const IChunkStorePtr& store) override
    {
        LogEvent("set_store_preload_state")
            .Item("store_id").Value(store->GetId())
            .Item("preload_state").Value(store->GetPreloadState());
    }

    void OnStoreFlushStateChanged(const IDynamicStorePtr& store) override
    {
        LogEvent("set_store_flush_state")
            .Item("store_id").Value(store->GetId())
            .Item("flush_state").Value(store->GetFlushState());
    }

    void OnPartitionSplit(
        const TPartition* oldPartition,
        int partitionIndex,
        int splitFactor) override
    {
        auto newPartitions = MakeRange(Tablet_->PartitionList())
            .Slice(partitionIndex, partitionIndex + splitFactor);
        LogEvent("split_partition")
            .Item("old_partition_id").Value(oldPartition->GetId())
            .Item("new_partitions").DoListFor(
                newPartitions,
                [&] (auto fluent, const auto& partition) {
                    fluent
                        .Item().BeginMap()
                            .Item("partition_id").Value(partition->GetId())
                            .Item("pivot_key").Value(partition->GetPivotKey())
                            .Item("sample_key_count").Value(partition->GetSampleKeys()->Keys.Size())
                        .EndMap();
                })
            // NB: deducible.
            .Item("new_store_partitions").DoMapFor(
                oldPartition->Stores(),
                [&] (auto fluent, const auto& store) {
                    fluent
                        .Item(ToString(store->GetId())).Value(store->GetPartition()->GetId());
                });
    }

    void OnPartitionsMerged(
        const std::vector<TPartitionId>& oldPartitionIds,
        const TPartition* newPartition) override
    {
        LogEvent("merge_partitions")
            .Item("old_partition_ids").List(oldPartitionIds)
            .Item("new_partition_id").Value(newPartition->GetId());
    }

    void OnImmediatePartitionSplitRequested(const TPartition* partition) override
    {
        LogEvent("request_immediate_split")
            .Item("partition_id").Value(partition->GetId())
            .Item("keys").List(partition->PivotKeysForImmediateSplit());
    }

private:
    const TStructuredLoggerPtr Logger_;
    TTablet* const Tablet_;
    const TTabletId TabletId_;
    std::atomic_bool Enabled_;

    template <class T>
    TGuid GetObjectId(const T& object)
    {
        return object ? object->GetId() : TGuid{};
    }

    void OnPartitionFullHeartbeat(const TPartition* partition, TFluentMap fluent)
    {
        // Looks like orchid? Yes it does.
        fluent
            .Item("id").Value(partition->GetId())
            .Item("state").Value(partition->GetState())
            .Item("pivot_key").Value(partition->GetPivotKey())
            .Item("next_pivot_key").Value(partition->GetNextPivotKey())
            .Item("sample_key_count").Value(partition->GetSampleKeys()->Keys.Size())
            .Item("sampling_time").Value(partition->GetSamplingTime())
            .Item("sampling_request_time").Value(partition->GetSamplingRequestTime())
            .Item("compaction_time").Value(partition->GetCompactionTime())
            .Item("allowed_split_time").Value(partition->GetAllowedSplitTime())
            .Item("allowed_merge_time").Value(partition->GetAllowedMergeTime())
            .Item("stores").DoMapFor(partition->Stores(), [&] (auto fluent, const IStorePtr& store) {
                fluent
                    .Item(ToString(store->GetId()))
                    .DoMap(BIND(
                        &TPerTabletStructuredLogger::OnStoreFullHeartbeat,
                        Unretained(this),
                        store));
            })
            .DoIf(partition->IsImmediateSplitRequested(), [&] (auto fluent) {
                fluent
                    .Item("immediate_split_keys").List(
                        partition->PivotKeysForImmediateSplit());
            });
    }

    void OnStoreFullHeartbeat(const IStorePtr& store, TFluentMap fluent)
    {
        fluent
            .Item("store_state").Value(store->GetStoreState())
            .Item("min_timestamp").Value(store->GetMinTimestamp())
            .Item("max_timestamp").Value(store->GetMaxTimestamp());

        if (store->IsSorted()) {
            OnSortedStoreFullHeartbeat(store->AsSorted(), fluent);
        } else {
            OnOrderedStoreFullHeartbeat(store->AsOrdered(), fluent);
        }

        if (store->IsDynamic()) {
            OnDynamicStoreFullHeartbeat(store->AsDynamic(), fluent);
        } else {
            OnChunkStoreFullHeartbeat(store->AsChunk(), fluent);
        }
    }

    void OnSortedStoreFullHeartbeat(const ISortedStorePtr& store, TFluentMap fluent)
    {
        if (store->IsChunk()) {
            auto chunkStore = store->AsSortedChunk();
            fluent
                .Item("min_key").Value(store->GetMinKey())
                .Item("upper_bound_key").Value(store->GetUpperBoundKey());
            if (auto chunkId = chunkStore->GetChunkId(); chunkId != store->GetId()) {
                // TODO: real chunk boundaries
                fluent
                    .Item("chunk_id").Value(chunkId);
                if (auto overrideTimestamp = chunkStore->GetOverrideTimestamp()) {
                    fluent
                        .Item("override_timestamp").Value(overrideTimestamp);
                }
            }
        }
    }

    void OnOrderedStoreFullHeartbeat(const IOrderedStorePtr& store, TFluentMap fluent)
    {
        fluent
            .Item("starting_row_index").Value(store->GetStartingRowIndex());
    }

    void OnDynamicStoreFullHeartbeat(const IDynamicStorePtr& store, TFluentMap fluent)
    {
        fluent
            .Item("flush_state").Value(store->GetFlushState())
            .Item("row_count").Value(store->GetRowCount())
            .Item("lock_count").Value(store->GetLockCount())
            .Item("value_count").Value(store->GetValueCount())
            .Item("pool_size").Value(store->GetPoolSize())
            .Item("pool_capacity").Value(store->GetPoolCapacity())
            .Item("last_flush_attempt_time").Value(store->GetLastFlushAttemptTimestamp());
    }

    void OnChunkStoreFullHeartbeat(const IChunkStorePtr& store, TFluentMap fluent)
    {
        auto backingStore = store->GetBackingStore();
        fluent
            .Item("preload_state").Value(store->GetPreloadState())
            .Item("compaction_state").Value(store->GetCompactionState())
            .Item("compressed_data_size").Value(store->GetCompressedDataSize())
            .Item("uncompressed_data_size").Value(store->GetUncompressedDataSize())
            .Item("row_count").Value(store->GetRowCount())
            .Item("creation_time").Value(store->GetCreationTime())
            .DoIf(backingStore.operator bool(), [&] (auto fluent) {
                fluent
                    .Item("backing_store").DoMap([&] (auto fluent) {
                        fluent
                            .Item(ToString(backingStore->GetId()))
                            .DoMap(BIND(
                                &TPerTabletStructuredLogger::OnStoreFullHeartbeat,
                                Unretained(this),
                                backingStore));
                    });
            });
    }

    void OnHunkChunkFullHeartbeat(const THunkChunkPtr& hunkChunk, TFluentMap fluent)
    {
        fluent
            .Item("hunk_count").Value(hunkChunk->GetHunkCount())
            .Item("total_hunk_length").Value(hunkChunk->GetTotalHunkLength());
    }

    void OnDynamicStoreIncrementalHeartbeat(const IDynamicStorePtr& store, TFluentMap fluent)
    {
        fluent
            .Item("row_count").Value(store->GetRowCount())
            .Item("lock_count").Value(store->GetLockCount())
            .Item("value_count").Value(store->GetValueCount())
            .Item("pool_size").Value(store->GetPoolSize())
            .Item("pool_capacity").Value(store->GetPoolCapacity());
    }
};

DEFINE_REFCOUNTED_TYPE(TPerTabletStructuredLogger)

////////////////////////////////////////////////////////////////////////////////

IPerTabletStructuredLoggerPtr TStructuredLogger::CreateLogger(TTablet* tablet)
{
    return New<TPerTabletStructuredLogger>(MakeStrong(this), tablet);
}

////////////////////////////////////////////////////////////////////////////////

IStructuredLoggerPtr CreateStructuredLogger(IBootstrap* bootstrap)
{
    return New<TStructuredLogger>(bootstrap);
}

IPerTabletStructuredLoggerPtr CreateMockPerTabletStructuredLogger(TTablet* tablet)
{
    return New<TPerTabletStructuredLogger>(tablet);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
