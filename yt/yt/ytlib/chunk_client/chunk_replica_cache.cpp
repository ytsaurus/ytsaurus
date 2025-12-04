#include "chunk_replica_cache.h"

#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/ytlib/cell_master_client/cell_directory.h>

#include <yt/yt/ytlib/sequoia_client/client.h>
#include <yt/yt/ytlib/sequoia_client/connection.h>
#include <yt/yt/ytlib/sequoia_client/records/chunk_replicas.record.h>
#include <yt/yt/ytlib/sequoia_client/records/unapproved_chunk_replicas.record.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/record_helpers.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <util/thread/lfstack.h>

#include <deque>

namespace NYT::NChunkClient {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NNodeTrackerClient;
using namespace NSequoiaClient;
using namespace NTableClient;
using namespace NApi;
using namespace NProfiling;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

class TChunkReplicaCache
    : public IChunkReplicaCache
{
public:
    TChunkReplicaCache(
        NApi::NNative::IConnectionPtr connection,
        const TProfiler& profiler,
        IMemoryUsageTrackerPtr memoryUsageTracker)
        : Connection_(connection)
        , NodeDirectory_(connection->GetNodeDirectory())
        , Logger(connection->GetLogger())
        , HitsCounter_(profiler.Counter("/hits"))
        , MissesCounter_(profiler.Counter("/misses"))
        , DiscardsCounter_(profiler.Counter("/discards"))
        , UpdatesCounter_(profiler.Counter("/updates"))
        , MasterLocateCallsCounter_(profiler.Counter("/master_locate_calls"))
        , MasterLocateChunksCounter_(profiler.Counter("/master_locate_chunks"))
        , SequoiaLocateCallsCounter_(profiler.Counter("/sequoia_locate_calls"))
        , SequoiaLocateChunksCounter_(profiler.Counter("/sequoia_locate_chunks"))
        , ExpiredChunksCounter_(profiler.Counter("/expired_chunks"))
        , MasterErrorDiscardsCounter_(profiler.Counter("/master_error_discards"))
        , CacheSizeGauge_(profiler.Gauge("/cache_size"))
        , ExpirationExecutor_(New<TPeriodicExecutor>(
            connection->GetInvoker(),
            BIND(&TChunkReplicaCache::OnExpirationSweep, MakeWeak(this))))
        , MemoryGuard_(TMemoryUsageTrackerGuard::Build(std::move(memoryUsageTracker)))
    {
        Reconfigure(
            connection->GetStaticConfig()->ChunkReplicaCache->ApplyDynamic(
                connection->GetConfig()->ChunkReplicaCache));
    }

    void Initialize()
    {
        ExpirationExecutor_->Start();
        ScheduleRefreshRound(TDuration::Zero());
    }

    std::vector<TErrorOr<TAllyReplicasInfo>> FindReplicas(
        const std::vector<TChunkId>& chunkIds) override
    {
        std::vector<TErrorOr<TAllyReplicasInfo>> replicas(chunkIds.size());

        auto now = TInstant::Now();
        auto entriesGuard = ReaderGuard(EntriesLock_);

        for (int index = 0; index < std::ssize(chunkIds); ++index) {
            auto chunkId = chunkIds[index];
            bool hit = false;
            YT_VERIFY(IsPhysicalChunkType(TypeFromId(chunkId)));
            auto it = Entries_.find(chunkId);
            if (it != Entries_.end()) {
                auto& entry = *it->second;
                auto entryGuard = Guard(entry.Lock);
                if (auto optionalExistingReplicas = entry.Future.TryGet()) {
                    if (optionalExistingReplicas->IsOK()) {
                        entry.LastAccessTime = now;
                        hit = true;
                    }
                    replicas[index] = *optionalExistingReplicas;
                }
            }

            if (hit) {
                HitsCounter_.Increment();
            } else {
                MissesCounter_.Increment();
            }
        }

        return replicas;
    }

    std::vector<TFuture<TAllyReplicasInfo>> GetReplicas(
        const std::vector<TChunkId>& chunkIds) override
    {
        std::vector<TFuture<TAllyReplicasInfo>> futures(chunkIds.size());
        std::vector<int> missingIndices;

        int hitCount = 0;
        int missCount = 0;

        auto now = TInstant::Now();

        {
            auto entriesGuard = ReaderGuard(EntriesLock_);
            for (int index = 0; index < std::ssize(chunkIds); ++index) {
                auto chunkId = chunkIds[index];
                YT_VERIFY(IsPhysicalChunkType(TypeFromId(chunkId)));
                auto it = Entries_.find(chunkId);
                if (it == Entries_.end()) {
                    missingIndices.push_back(index);
                } else {
                    ++hitCount;
                    auto& entry = *it->second;
                    auto entryGuard = Guard(entry.Lock);
                    entry.LastAccessTime = now;
                    futures[index] = entry.Future;
                }
            }
        }

        std::vector<int> stillMissingIndices;
        std::vector<TPromise<TAllyReplicasInfo>> stillMissingPromises(chunkIds.size());

        if (!missingIndices.empty()) {
            auto entriesGuard = WriterGuard(EntriesLock_);
            for (int index = 0; index < std::ssize(chunkIds); ++index) {
                auto chunkId = chunkIds[index];
                auto it = Entries_.find(chunkId);
                if (it == Entries_.end()) {
                    ++missCount;
                    auto entry = std::make_unique<TEntry>();
                    entry->LastAccessTime = now;
                    auto promise = entry->Promise = NewPromise<TAllyReplicasInfo>();
                    entry->Future = entry->Promise.ToFuture().ToUncancelable();
                    it = EmplaceOrCrash(Entries_, chunkId, std::move(entry));
                    NewlyAddedChunkIds_.Enqueue(chunkId);
                    stillMissingPromises[index] = std::move(promise);
                    stillMissingIndices.push_back(index);
                } else {
                    ++hitCount;
                }

                const auto& entry = *it->second;
                {
                    auto entryGuard = Guard(entry.Lock);
                    futures[index] = entry.Future;
                }
            }

            OnSizeUpdated();
        }

        HitsCounter_.Increment(hitCount);
        MissesCounter_.Increment(missCount);

        if (!missingIndices.empty()) {
            auto handler = EnableSequoiaReplicasLocate_.load(std::memory_order::relaxed)
                ? &TChunkReplicaCache::SequoiaLocateReplicas
                : &TChunkReplicaCache::MasterLocateReplicas;
            (this->*handler)(std::move(chunkIds), std::move(stillMissingIndices), std::move(stillMissingPromises));
        }

        return futures;
    }

    void MasterLocateReplicas(
        std::vector<TChunkId> chunkIds,
        std::vector<int> missingIndices,
        std::vector<TPromise<TAllyReplicasInfo>> promises)
    {
        auto connection = Connection_.Lock();
        if (!connection) {
            return;
        }

        THashMap<TCellTag, std::vector<int>> cellTagToMissingIndices;
        for (auto index : missingIndices) {
            auto chunkId = chunkIds[index];
            cellTagToMissingIndices[CellTagFromId(chunkId)].push_back(index);
        }

        auto maxChunksPerLocate = MaxChunksPerMasterLocate_.load(std::memory_order::relaxed);

        for (const auto& [cellTag, missingIndicesPerCell] : cellTagToMissingIndices) {
            try {
                auto channel = connection->GetMasterCellDirectory()->GetMasterChannelOrThrow(
                    EMasterChannelKind::Follower,
                    cellTag);

                TChunkServiceProxy proxy(std::move(channel));
                TChunkServiceProxy::TReqLocateChunksPtr currentReq;

                std::vector<TChunkId> currentChunkIds;
                std::vector<TPromise<TAllyReplicasInfo>> currentPromises;

                auto flushCurrent = [&] {
                    if (!currentReq) {
                        return;
                    }

                    YT_LOG_DEBUG("Locating chunks at master (CellTag: %v, ChunkIds: %v)",
                        cellTag,
                        currentChunkIds);

                    MasterLocateCallsCounter_.Increment();
                    MasterLocateChunksCounter_.Increment(std::ssize(currentChunkIds));

                    currentReq->Invoke()
                        .Subscribe(BIND(&TChunkReplicaCache::OnMasterReplicasLocated,
                            MakeStrong(this),
                            cellTag,
                            std::move(currentChunkIds),
                            std::move(currentPromises)));

                    currentReq.Reset();
                    currentChunkIds.clear();
                    currentPromises.clear();
                };

                for (auto index : missingIndicesPerCell) {
                    auto chunkId = chunkIds[index];
                    currentChunkIds.push_back(chunkId);
                    currentPromises.push_back(promises[index]);

                    if (!currentReq) {
                        currentReq = proxy.LocateChunks();
                        currentReq->SetResponseHeavy(true);
                    }

                    ToProto(currentReq->add_subrequests(), chunkId);

                    if (std::ssize(currentChunkIds) >= maxChunksPerLocate) {
                        flushCurrent();
                    }
                }

                flushCurrent();
            } catch (const std::exception& ex) {
                // NB: GetMasterChannelOrThrow above may throw.

                {
                    auto error = TError(ex);
                    for (auto index : missingIndicesPerCell) {
                        promises[index].TrySet(error);
                    }
                }


                int errorCount = 0;
                {
                    // Errors must not be sticky; evict promises.
                    auto entriesGuard = WriterGuard(EntriesLock_);
                    for (auto index : missingIndicesPerCell) {
                        auto chunkId = chunkIds[index];
                        if (auto it = Entries_.find(chunkId); it != Entries_.end()) {
                            auto& entry = *it->second;
                            auto entryGuard = Guard(entry.Lock);
                            if (entry.Promise == promises[index]) {
                                entryGuard.Release();
                                Entries_.erase(it);
                                ++errorCount;
                            }
                        }
                    }

                    OnSizeUpdated();
                }

                MasterErrorDiscardsCounter_.Increment(errorCount);
            }
        }
    }

    void SequoiaLocateReplicas(
        std::vector<TChunkId> chunkIds,
        std::vector<int> missingIndices,
        std::vector<TPromise<TAllyReplicasInfo>> promises)
    {
        auto connection = Connection_.Lock();
        if (!connection) {
            return;
        }

        const auto& idMapping = NSequoiaClient::NRecords::TChunkReplicasDescriptor::Get()->GetIdMapping();
        TColumnFilter columnFilter{idMapping.StoredReplicas};

        std::vector<TChunkId> currentChunkIds;
        std::vector<TPromise<TAllyReplicasInfo>> currentPromises;

        auto flushCurrent = [&] {
            if (currentChunkIds.empty()) {
                return;
            }

            YT_LOG_DEBUG("Locating chunks in Sequoia (ChunkIds: %v)",
                currentChunkIds);

            SequoiaLocateCallsCounter_.Increment();
            SequoiaLocateChunksCounter_.Increment(std::ssize(currentChunkIds));

            LookupReplicasInSequoia(connection, currentChunkIds)
                .AsUnique().Subscribe(BIND(
                    &TChunkReplicaCache::OnSequoiaReplicasLocated,
                    MakeStrong(this),
                    std::move(currentChunkIds),
                    std::move(currentPromises)));

            currentChunkIds.clear();
            currentPromises.clear();
        };

        auto maxChunksPerLocate = MaxChunksPerMasterLocate_.load(std::memory_order::relaxed);

        for (auto index : missingIndices) {
            auto chunkId = chunkIds[index];
            currentChunkIds.push_back(chunkId);
            currentPromises.push_back(promises[index]);
            if (std::ssize(currentChunkIds) >= maxChunksPerLocate) {
                flushCurrent();
            }
        }

        flushCurrent();
    }

    void DiscardReplicas(
        TChunkId chunkId,
        const TFuture<TAllyReplicasInfo>& future) override
    {
        YT_VERIFY(IsPhysicalChunkType(TypeFromId(chunkId)));

        auto now = TInstant::Now();
        auto entriesGuard = WriterGuard(EntriesLock_);
        auto it = Entries_.find(chunkId);
        if (it != Entries_.end()) {
            auto& entry = *it->second;
            auto entryGuard = Guard(entry.Lock);
            entry.LastAccessTime = now;
            if (entry.Future == future) {
                entryGuard.Release();
                Entries_.erase(it);
                YT_LOG_DEBUG("Chunk replicas discarded from replica cache (ChunkId: %v)",
                    chunkId);
                DiscardsCounter_.Increment();
            }
        }

        OnSizeUpdated();
    }

    void PingChunks(const std::vector<TChunkId>& chunkIds) override
    {
        auto now = TInstant::Now();
        auto entriesGuard = ReaderGuard(EntriesLock_);

        for (const auto& chunkId : chunkIds) {
            YT_VERIFY(IsPhysicalChunkType(TypeFromId(chunkId)));

            if (auto it = Entries_.find(chunkId); it != Entries_.end()) {
                const auto& entry = it->second;
                auto entryGuard = Guard(entry->Lock);
                if (entry->LastAccessTime < now) {
                    entry->LastAccessTime = now;
                }
            }
        }
    }

    void UpdateReplicas(
        TChunkId chunkId,
        const TAllyReplicasInfo& replicas) override
    {
        YT_VERIFY(IsPhysicalChunkType(TypeFromId(chunkId)));

        auto now = TInstant::Now();

        // Make sure the order of replicas does not matter; see the check below.
        auto canonicalReplicas = replicas;
        std::ranges::sort(canonicalReplicas.Replicas);

        auto update = [&] (TEntry* entry) {
            entry->LastAccessTime = now;

            // TEntry::Future is being used as a key in #DiscardReplicas; see YT-26345.
            // Try to preserve it as long as the replica set remains same.
            if (entry->Future &&
                entry->Future.IsSet() &&
                entry->Future.Get().IsOK() &&
                entry->Future.Get().Value() == canonicalReplicas)
            {
                return;
            }

            entry->Promise = MakePromise(canonicalReplicas);
            entry->Future = entry->Promise.ToFuture().ToUncancelable();

            YT_LOG_DEBUG("Chunk replicas updated (ChunkId: %v, Replicas: %v, Revision: %x)",
                chunkId,
                MakeFormattableView(canonicalReplicas.Replicas, TChunkReplicaAddressFormatter(NodeDirectory_)),
                canonicalReplicas.Revision);

            UpdatesCounter_.Increment();
        };

        auto tryUpdate = [&] (TEntry* entry) {
            auto entryGuard = Guard(entry->Lock);

            auto oldRevision = NHydra::NullRevision;
            if (auto optionalExistingReplicas = entry->Future.TryGet()) {
                if (optionalExistingReplicas->IsOK()) {
                    oldRevision = optionalExistingReplicas->Value().Revision;
                }
            }

            // NB: Sequoia replicas always use TAllyReplicasInfo::SequoiaRevision;
            // in case of a tie, consider these to be fresh replicas.
            if (oldRevision >= canonicalReplicas.Revision &&
                canonicalReplicas.Revision != TAllyReplicasInfo::SequoiaRevision)
            {
                return;
            }

            update(entry);
        };

        {
            auto entriesGuard = ReaderGuard(EntriesLock_);
            auto it = Entries_.find(chunkId);
            if (it != Entries_.end()) {
                tryUpdate(it->second.get());
                return;
            }
        }

        {
            auto entriesGuard = WriterGuard(EntriesLock_);
            auto it = Entries_.find(chunkId);
            if (it == Entries_.end()) {
                it = EmplaceOrCrash(Entries_, chunkId, std::make_unique<TEntry>());
                NewlyAddedChunkIds_.Enqueue(chunkId);
                update(it->second.get());
            } else {
                tryUpdate(it->second.get());
            }

            OnSizeUpdated();
        }
    }

    void Reconfigure(TChunkReplicaCacheConfigPtr config) override
    {
        auto connection = Connection_.Lock();
        if (!connection) {
            return;
        }

        ExpirationExecutor_->SetPeriod(config->ExpirationSweepPeriod);
        ExpirationTime_.store(config->ExpirationTime);
        MaxChunksPerMasterLocate_.store(config->MaxChunksPerMasterLocate);
        bool sequoiaConfigured = connection->IsSequoiaConfigured();
        EnableSequoiaReplicasLocate_.store(sequoiaConfigured && config->EnableSequoiaReplicasLocate);
        EnableSequoiaReplicasRefresh_.store(sequoiaConfigured && config->EnableSequoiaReplicasRefresh);
        MaxChunksPerSequoiaRefreshRound_.store(config->MaxChunksPerSequoiaRefreshRound);
        SequoiaReplicasRefreshPeriod_.store(config->SequoiaReplicasRefreshPeriod);
    }

private:
    const TWeakPtr<NApi::NNative::IConnection> Connection_;
    const TNodeDirectoryPtr NodeDirectory_;
    const NLogging::TLogger Logger;

    const TCounter HitsCounter_;
    const TCounter MissesCounter_;
    const TCounter DiscardsCounter_;
    const TCounter UpdatesCounter_;
    const TCounter MasterLocateCallsCounter_;
    const TCounter MasterLocateChunksCounter_;
    const TCounter SequoiaLocateCallsCounter_;
    const TCounter SequoiaLocateChunksCounter_;
    const TCounter ExpiredChunksCounter_;
    const TCounter MasterErrorDiscardsCounter_;
    const TGauge CacheSizeGauge_;

    const TPeriodicExecutorPtr ExpirationExecutor_;

    std::atomic<TDuration> ExpirationTime_;
    std::atomic<int> MaxChunksPerMasterLocate_;
    std::atomic<bool> EnableSequoiaReplicasLocate_;
    std::atomic<bool> EnableSequoiaReplicasRefresh_;
    std::atomic<int> MaxChunksPerSequoiaRefreshRound_ = 1;
    std::atomic<TDuration> SequoiaReplicasRefreshPeriod_;

    struct TEntry
    {
        YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock);
        TInstant LastAccessTime;
        TPromise<TAllyReplicasInfo> Promise;
        TFuture<TAllyReplicasInfo> Future;
    };

    // TODO(babenko): maybe implement sharding
    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, EntriesLock_);
    THashMap<TChunkId, std::unique_ptr<TEntry>> Entries_;
    TMemoryUsageTrackerGuard MemoryGuard_;

    TLockFreeStack<TChunkId> NewlyAddedChunkIds_;
    static constexpr TChunkId GenerationSentinel = TChunkId();
    std::deque<TChunkId> RefreshQueue_{GenerationSentinel};

    TInstant CurrentGenerationStartInstant_;
    int ChunksRefreshedInCurrentGeneration_ = 0;

    template <class TReplicasInfo>
    void UpdateEntryReplicas(TEntry* entry, TReplicasInfo&& replicasInfo)
    {
        YT_ASSERT_SPINLOCK_AFFINITY(entry->Lock);
        // It's racy but still OK.
        if (entry->Promise.IsSet()) {
            entry->Promise = MakePromise<TAllyReplicasInfo>(std::forward<TReplicasInfo>(replicasInfo));
            entry->Future = entry->Promise.ToFuture().ToUncancelable();
        } else {
            entry->Promise.TrySet(std::forward<TReplicasInfo>(replicasInfo));
        }
    }

    void OnMasterReplicasLocated(
        TCellTag cellTag,
        const std::vector<TChunkId>& chunkIds,
        const std::vector<TPromise<TAllyReplicasInfo>>& promises,
        const TChunkServiceProxy::TErrorOrRspLocateChunksPtr& rspOrError)
    {
        auto connection = Connection_.Lock();
        if (!connection) {
            return;
        }

        if (!rspOrError.IsOK()) {
            YT_LOG_WARNING(rspOrError, "Error locating chunks at master (CellTag: %v)",
                cellTag);

            OnLocateChunksFailed(chunkIds, promises, rspOrError);
            return;
        }

        YT_LOG_DEBUG("Chunks located at master (CellTag: %v, ChunkCount: %v)",
            cellTag,
            std::ssize(promises));

        const auto& rsp = rspOrError.Value();

        NodeDirectory_->MergeFrom(rsp->node_directory());

        for (int index = 0; index < std::ssize(chunkIds); ++index) {
            const auto& subresponse = rsp->subresponses(index);
            if (subresponse.missing()) {
                auto chunkId = chunkIds[index];
                YT_LOG_DEBUG("Chunk is missing at master (ChunkId: %v)", chunkId);
                promises[index].TrySet(TError(
                    NChunkClient::EErrorCode::NoSuchChunk,
                    "No such chunk %v",
                    chunkId));
                continue;
            }

            auto replicas = FromProto<TChunkReplicaWithMediumList>(subresponse.replicas());
            auto revision = FromProto<NHydra::TRevision>(rsp->revision());
            auto replicasInfo = TAllyReplicasInfo::FromChunkReplicas(replicas, revision);
            promises[index].TrySet(std::move(replicasInfo));
        }
    }

    TFuture<std::vector<std::optional<TAllyReplicasInfo>>> LookupReplicasInSequoia(
        const NApi::NNative::IConnectionPtr& connection,
        const std::vector<TChunkId>& chunkIds)
    {
        auto client = connection->GetSequoiaConnection()->CreateClient(NRpc::GetRootAuthenticationIdentity());

        auto approvedReplicasFuture = [&] {
            std::vector<NSequoiaClient::NRecords::TChunkReplicasKey> recordKeys;
            recordKeys.reserve(chunkIds.size());
            for (auto chunkId : chunkIds) {
                recordKeys.push_back({.ChunkId = chunkId});
            }

            const auto& idMapping = NSequoiaClient::NRecords::TChunkReplicasDescriptor::Get()->GetIdMapping();
            TColumnFilter columnFilter{idMapping.StoredReplicas};

            return client->LookupRows(recordKeys, columnFilter);
        }();

        auto unapprovedReplicasFuture = [&] {
            std::vector<NSequoiaClient::NRecords::TUnapprovedChunkReplicasKey> recordKeys;
            recordKeys.reserve(chunkIds.size());
            for (auto chunkId : chunkIds) {
                recordKeys.push_back({.ChunkId = chunkId});
            }

            const auto& idMapping = NSequoiaClient::NRecords::TUnapprovedChunkReplicasDescriptor::Get()->GetIdMapping();
            TColumnFilter columnFilter{idMapping.StoredReplicas};

            return client->LookupRows(recordKeys, columnFilter);
        }();

        return AllSucceeded(std::vector{approvedReplicasFuture.AsVoid(), unapprovedReplicasFuture.AsVoid()})
            .Apply(BIND([=] {
                const auto& approvedReplicas = approvedReplicasFuture.Get().Value();
                const auto& unapprovedReplicas = unapprovedReplicasFuture.Get().Value();
                YT_VERIFY(approvedReplicas.size() == unapprovedReplicas.size());

                std::vector<std::optional<TAllyReplicasInfo>> results;
                for (int index = 0; index < std::ssize(approvedReplicas); ++index) {
                    const auto& optionalApprovedRecord = approvedReplicas[index];
                    const auto& optionalUnapprovedRecord = unapprovedReplicas[index];
                    auto& result = results.emplace_back();

                    auto handleRecord = [&] (const auto& optionalRecord) {
                        if (!optionalRecord) {
                            return;
                        }

                        if (!result) {
                            result = TAllyReplicasInfo{
                                .Revision = TAllyReplicasInfo::SequoiaRevision,
                            };
                        }

                        ParseChunkReplicas(
                            optionalRecord->StoredReplicas,
                            [&] (const TParsedChunkReplica& parsedReplica) {
                                result->Replicas.push_back(TChunkReplicaWithMedium(
                                    parsedReplica.NodeId,
                                    parsedReplica.ReplicaIndex,
                                    NChunkClient::DefaultStoreMediumIndex));
                            });
                    };
                    handleRecord(optionalApprovedRecord);
                    handleRecord(optionalUnapprovedRecord);
                }

                return results;
            }));
    }

    void OnSequoiaReplicasLocated(
        const std::vector<TChunkId>& chunkIds,
        const std::vector<TPromise<TAllyReplicasInfo>>& promises,
        TErrorOr<std::vector<std::optional<TAllyReplicasInfo>>>&& resultsOrError)
    {
        if (!resultsOrError.IsOK()) {
            YT_LOG_WARNING(resultsOrError, "Error locating chunks in Sequoia");

            OnLocateChunksFailed(chunkIds, promises, resultsOrError);
            return;
        }

        auto& results = resultsOrError.Value();

        YT_LOG_DEBUG("Chunks located in Sequoia (ChunkCount: %v)",
            std::ssize(promises));

        for (int index = 0; index < std::ssize(chunkIds); ++index) {
            auto chunkId = chunkIds[index];
            const auto& promise = promises[index];
            auto& optionalResult = results[index];

            if (!optionalResult) {
                YT_LOG_DEBUG("Chunk is missing in Sequoia (ChunkId: %v)", chunkId);
                promise.TrySet(TError(
                    NChunkClient::EErrorCode::NoSuchChunk,
                    "No such chunk %v",
                    chunkId));
                continue;
            }

            promise.TrySet(std::move(*optionalResult));
        }
    }

    void OnLocateChunksFailed(
        const std::vector<TChunkId>& chunkIds,
        const std::vector<TPromise<TAllyReplicasInfo>>& promises,
        const TError& error)
    {
        {
            auto entriesGuard = WriterGuard(EntriesLock_);
            for (int index = 0; index < std::ssize(chunkIds); ++index) {
                auto chunkId = chunkIds[index];
                auto it = Entries_.find(chunkId);
                if (it == Entries_.end()) {
                    continue;
                }
                Entries_.erase(it);
            }

            OnSizeUpdated();
        }

        for (const auto& promise : promises) {
            promise.TrySet(error);
        }
    }

    void OnExpirationSweep()
    {
        std::vector<TChunkId> expiredChunkIds;
        auto deadline = TInstant::Now() - ExpirationTime_.load(std::memory_order::relaxed);

        YT_LOG_DEBUG("Started expired chunk replica sweep (Deadline: %v)",
            deadline);

        int totalChunkCount;

        {
            auto entriesGuard = ReaderGuard(EntriesLock_);
            totalChunkCount = std::ssize(Entries_);
            for (const auto& [chunkId, entry] : Entries_) {
                auto entryGuard = Guard(entry->Lock);
                if (entry->LastAccessTime < deadline) {
                    expiredChunkIds.push_back(chunkId);
                }
            }
        }

        ExpiredChunksCounter_.Increment(expiredChunkIds.size());

        if (!expiredChunkIds.empty()) {
            auto entriesGuard = WriterGuard(EntriesLock_);
            for (auto chunkId : expiredChunkIds) {
                Entries_.erase(chunkId);
            }
            OnSizeUpdated();
        }

        YT_LOG_DEBUG("Finished expired chunk replica sweep (TotalChunkCount: %v, ExpiredChunkCount: %v)",
            totalChunkCount,
            expiredChunkIds.size());
    }

    void ScheduleRefreshRound(TDuration delay)
    {
        if (auto connection = Connection_.Lock()) {
            TDelayedExecutor::Submit(
                BIND(&TChunkReplicaCache::OnRefreshRound, MakeWeak(this)),
                delay,
                connection->GetInvoker());
        }
    }


    void ScheduleNextRefreshRound()
    {
        int chunksTotal = [&] {
            auto entriesGuard = WriterGuard(EntriesLock_);
            return std::ssize(Entries_);
        }();

        int chunksRefreshed = ChunksRefreshedInCurrentGeneration_;

        // Avoid dealing with edges cases below.
        int adjustedChunksTotal = std::max(chunksTotal, 1);
        int adjustedChunksRefreshed = std::clamp(chunksRefreshed, 1, adjustedChunksTotal);

        // Try to smooth replica fetch across refresh period.
        auto period = SequoiaReplicasRefreshPeriod_.load(std::memory_order::relaxed);
        auto expectedNextRoundStartOffset = period * static_cast<double>(adjustedChunksRefreshed) / adjustedChunksTotal;
        auto delay =  CurrentGenerationStartInstant_ + expectedNextRoundStartOffset - TInstant::Now();

        YT_LOG_DEBUG("Chunk refresh round finished "
            "(CurrentGenerationStartInstant: %v, ChunksRefreshedInCurrentGeneration: %v, "
            "ChunksTotal: %v, DelayBeforeNextRound: %v)",
            CurrentGenerationStartInstant_,
            ChunksRefreshedInCurrentGeneration_,
            chunksTotal,
            delay);

        ScheduleRefreshRound(delay);
    }

    void OnRefreshRound()
    {
        auto connection = Connection_.Lock();
        if (!connection) {
            return;
        }

        std::vector<TChunkId> newlyAddedChunkIds;
        NewlyAddedChunkIds_.DequeueAllSingleConsumer(&newlyAddedChunkIds);

        for (auto chunkId : newlyAddedChunkIds) {
            RefreshQueue_.push_back(chunkId);
        }

        int maxChunks = MaxChunksPerSequoiaRefreshRound_.load(std::memory_order::relaxed);
        int chunksDequeued = 0;
        int chunksEnqueued = 0;
        std::vector<TChunkId> chunkIdsToRefresh;
        std::vector<TChunkId> chunkIdsToReenqueue;
        bool generationStarted = false;
        {
            auto entriesGuard = ReaderGuard(EntriesLock_);
            while (!RefreshQueue_.empty() && std::ssize(chunkIdsToRefresh) < maxChunks && !generationStarted) {
                auto chunkId = RefreshQueue_.front();
                RefreshQueue_.pop_front();
                if (chunkId == GenerationSentinel) {
                    // Re-enqueue the sentinel.
                    chunkIdsToReenqueue.push_back(GenerationSentinel);
                    generationStarted = true;
                } else {
                    ++chunksDequeued;
                    if (Entries_.contains(chunkId)) {
                        chunkIdsToRefresh.push_back(chunkId);
                        chunkIdsToReenqueue.push_back(chunkId);
                        ++chunksEnqueued;
                    }
                }
            }
        }

        for (auto chunkId : chunkIdsToReenqueue) {
            RefreshQueue_.push_back(chunkId);
        }

        YT_LOG_DEBUG("Chunk refresh round started (ChunksNewlyAdded: %v, ChunksDequeued: %v, ChunksEnqueued: %v, GenerationStarted: %v)",
            newlyAddedChunkIds.size(),
            chunksDequeued,
            chunksEnqueued,
            generationStarted);

        if (generationStarted) {
            ChunksRefreshedInCurrentGeneration_ = 0;
            CurrentGenerationStartInstant_ = TInstant::Now();
        }

        ChunksRefreshedInCurrentGeneration_ += std::ssize(chunkIdsToRefresh);

        if (EnableSequoiaReplicasRefresh_.load(std::memory_order::relaxed)) {
            const auto& idMapping = NSequoiaClient::NRecords::TChunkReplicasDescriptor::Get()->GetIdMapping();
            TColumnFilter columnFilter{idMapping.StoredReplicas};

            YT_LOG_DEBUG("Refreshing chunks in Sequoia (ChunkCount: %v)",
                chunkIdsToRefresh.size());

            SequoiaLocateCallsCounter_.Increment();
            SequoiaLocateChunksCounter_.Increment(std::ssize(chunkIdsToRefresh));

            LookupReplicasInSequoia(connection, chunkIdsToRefresh)
                .AsUnique().Subscribe(BIND(
                    &TChunkReplicaCache::OnSequoiaReplicasRefreshed,
                    MakeStrong(this),
                    std::move(chunkIdsToRefresh)));
        } else {
            YT_LOG_DEBUG("Sequoia chunk refresh is disabled");
            ScheduleNextRefreshRound();
        }
    }

    void OnSequoiaReplicasRefreshed(
        const std::vector<TChunkId>& chunkIds,
        TErrorOr<std::vector<std::optional<TAllyReplicasInfo>>>&& resultsOrError)
    {
        if (resultsOrError.IsOK()) {
            YT_LOG_DEBUG("Chunks refreshed in Sequoia");

            auto& results = resultsOrError.Value();

            THashMap<TChunkId, TAllyReplicasInfo> chunkIdToReplicasInfo;
            for (int index = 0; index < std::ssize(chunkIds); ++index) {
                auto chunkId = chunkIds[index];
                auto& optionalResult = results[index];

                if (!optionalResult) {
                    continue;
                }

                chunkIdToReplicasInfo.emplace(chunkId, std::move(*optionalResult));
            }

            {
                auto entriesGuard = WriterGuard(EntriesLock_);
                for (auto& [chunkId, replicasInfo] : chunkIdToReplicasInfo) {
                    auto it = Entries_.find(chunkId);
                    if (it == Entries_.end()) {
                        continue;
                    }

                    const auto& entry = it->second;
                    {
                        auto entryGuard = Guard(entry->Lock);
                        // It's racy but still OK.
                        if (entry->Promise.IsSet()) {
                            entry->Promise = MakePromise<TAllyReplicasInfo>(std::move(replicasInfo));
                            entry->Future = entry->Promise.ToFuture().ToUncancelable();
                        } else {
                            entry->Promise.TrySet(std::move(replicasInfo));
                        }
                    }
                }
            }
        } else {
            YT_LOG_WARNING(resultsOrError, "Error refreshing chunks in Sequoia");
        }

        ScheduleNextRefreshRound();
    }

    void OnSizeUpdated()
    {
        YT_ASSERT_WRITER_SPINLOCK_AFFINITY(EntriesLock_);

        // Here size estimation relies on the fact that TCompactVector in TAllyReplicasInfo
        // does not stray too much from its specified expected size.
        MemoryGuard_.SetSize(Entries_.size() * (
            sizeof(TChunkId) +
            sizeof(std::unique_ptr<TEntry>) +
            sizeof(TEntry) +
            sizeof(TAllyReplicasInfo)));
        CacheSizeGauge_.Update(Entries_.size());
    }
};

////////////////////////////////////////////////////////////////////////////////

IChunkReplicaCachePtr CreateChunkReplicaCache(
    NApi::NNative::IConnectionPtr connection,
    TProfiler profiler,
    IMemoryUsageTrackerPtr memoryUsageTracker)
{
    auto cache = New<TChunkReplicaCache>(
        std::move(connection),
        std::move(profiler),
        std::move(memoryUsageTracker));
    cache->Initialize();
    return cache;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
