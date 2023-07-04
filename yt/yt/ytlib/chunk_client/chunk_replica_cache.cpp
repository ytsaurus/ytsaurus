#include "chunk_replica_cache.h"

#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/ytlib/cell_master_client/cell_directory.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

namespace NYT::NChunkClient {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NNodeTrackerClient;
using namespace NApi;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

class TChunkReplicaCache
    : public IChunkReplicaCache
{
public:
    explicit TChunkReplicaCache(NApi::NNative::IConnectionPtr connection)
        : Connection_(connection)
        , Config_(connection->GetConfig()->ChunkReplicaCache)
        , NodeDirectory_(connection->GetNodeDirectory())
        , Logger(connection->GetLogger())
        , ExpirationExecutor_(New<TPeriodicExecutor>(
            connection->GetInvoker(),
            BIND(&TChunkReplicaCache::OnExpirationSweep, MakeWeak(this)),
            Config_->ExpirationTime))
    {
        ExpirationExecutor_->Start();
    }

    std::vector<TErrorOr<TAllyReplicasInfo>> FindReplicas(
        const std::vector<TChunkId>& chunkIds) override
    {
        std::vector<TErrorOr<TAllyReplicasInfo>> replicas(chunkIds.size());

        auto now = TInstant::Now();

        auto mapGuard = ReaderGuard(EntriesLock_);
        for (int index = 0; index < std::ssize(chunkIds); ++index) {
            auto chunkId = chunkIds[index];
            YT_VERIFY(IsPhysicalChunkType(TypeFromId(chunkId)));
            auto it = Entries_.find(chunkId);
            if (it != Entries_.end()) {
                auto& entry = *it->second;
                auto entryGuard = Guard(entry.Lock);
                if (auto optionalExistingReplicas = entry.Future.TryGet()) {
                    if (optionalExistingReplicas->IsOK()) {
                        entry.LastAccessTime = now;
                    }
                    replicas[index] = *optionalExistingReplicas;
                }
            }
        }

        return replicas;
    }

    std::vector<TFuture<TAllyReplicasInfo>> GetReplicas(
        const std::vector<TChunkId>& chunkIds) override
    {
        std::vector<TFuture<TAllyReplicasInfo>> futures(chunkIds.size());
        std::vector<int> missingIndices;
        auto now = TInstant::Now();

        {
            auto mapGuard = ReaderGuard(EntriesLock_);
            for (int index = 0; index < std::ssize(chunkIds); ++index) {
                auto chunkId = chunkIds[index];
                YT_VERIFY(IsPhysicalChunkType(TypeFromId(chunkId)));
                auto it = Entries_.find(chunkId);
                if (it == Entries_.end()) {
                    missingIndices.push_back(index);
                } else {
                    auto& entry = *it->second;
                    auto entryGuard = Guard(entry.Lock);
                    entry.LastAccessTime = now;
                    futures[index] = entry.Future;
                }
            }
        }

        std::vector<TPromise<TAllyReplicasInfo>> promises(chunkIds.size());
        THashMap<TCellTag, std::vector<int>> cellTagToStillMissingIndices;

        if (!missingIndices.empty()) {
            auto mapGuard = WriterGuard(EntriesLock_);
            for (int index = 0; index < std::ssize(chunkIds); ++index) {
                auto chunkId = chunkIds[index];
                auto it = Entries_.find(chunkId);
                if (it == Entries_.end()) {
                    cellTagToStillMissingIndices[CellTagFromId(chunkId)].push_back(index);
                    it = EmplaceOrCrash(Entries_, chunkId, std::make_unique<TEntry>());
                    auto& entry = *it->second;
                    entry.LastAccessTime = now;
                    entry.Promise = NewPromise<TAllyReplicasInfo>();
                    entry.Future = entry.Promise.ToFuture().ToUncancelable();
                    promises[index] = entry.Promise;
                }
                auto& entry = *it->second;
                auto entryGuard = Guard(entry.Lock);
                futures[index] = entry.Future;
            }
        }

        if (!cellTagToStillMissingIndices.empty()) {
            auto connection = Connection_.Lock();
            if (!connection) {
                return futures;
            }

            for (auto& [cellTag, stillMissingIndices] : cellTagToStillMissingIndices) {
                try {
                    auto channel = connection->GetMasterCellDirectory()->GetMasterChannelOrThrow(
                        EMasterChannelKind::Follower,
                        cellTag);

                    TChunkServiceProxy proxy(std::move(channel));
                    TChunkServiceProxy::TReqLocateChunksPtr currentReq;

                    std::vector<TChunkId> currentChunkIds;
                    std::vector<TPromise<TAllyReplicasInfo>> currentPromises;

                    auto flushCurrent = [&, cellTag = cellTag] {
                        if (!currentReq) {
                            return;
                        }

                        YT_LOG_DEBUG("Locating chunks (CellTag: %v, ChunkIds: %v)",
                            cellTag,
                            currentChunkIds);

                        currentReq->Invoke().Subscribe(BIND(&TChunkReplicaCache::OnChunksLocated,
                            MakeStrong(this),
                            cellTag,
                            std::move(currentChunkIds),
                            std::move(currentPromises)));

                        currentReq.Reset();
                    };

                    for (auto index : stillMissingIndices) {
                        auto chunkId = chunkIds[index];
                        currentChunkIds.push_back(chunkId);
                        currentPromises.push_back(promises[index]);

                        if (!currentReq) {
                            currentReq = proxy.LocateChunks();
                            currentReq->SetResponseHeavy(true);
                        }

                        ToProto(currentReq->add_subrequests(), chunkId);

                        if (std::ssize(currentChunkIds) >= Config_->MaxChunksPerLocate) {
                            flushCurrent();
                        }
                    }

                    flushCurrent();
                } catch (const std::exception& ex) {
                    // NB: GetMasterChannelOrThrow above may throw.

                    auto error = TError(ex);
                    for (auto index : stillMissingIndices) {
                        promises[index].Set(error);
                    }

                    // Errors must not be sticky; evict promises.
                    auto mapGuard = WriterGuard(EntriesLock_);
                    for (auto index : stillMissingIndices) {
                        auto chunkId = chunkIds[index];
                        if (auto it = Entries_.find(chunkId); it != Entries_.end()) {
                            auto& entry = *it->second;
                            auto entryGuard = Guard(entry.Lock);
                            if (entry.Promise == promises[index]) {
                                entryGuard.Release();
                                Entries_.erase(it);
                            }
                        }
                    }
                }
            }
        }

        return futures;
    }

    void DiscardReplicas(
        TChunkId chunkId,
        const TFuture<TAllyReplicasInfo>& future) override
    {
        YT_VERIFY(IsPhysicalChunkType(TypeFromId(chunkId)));

        auto now = TInstant::Now();
        auto mapGuard = WriterGuard(EntriesLock_);
        auto it = Entries_.find(chunkId);
        if (it != Entries_.end()) {
            auto& entry = *it->second;
            auto entryGuard = Guard(entry.Lock);
            entry.LastAccessTime = now;
            if (entry.Future == future) {
                entryGuard.Release();
                Entries_.erase(it);
                YT_LOG_DEBUG("Chunk replicas discarded (ChunkId: %v)",
                    chunkId);
            }
        }
    }

    void UpdateReplicas(
        TChunkId chunkId,
        const TAllyReplicasInfo& replicas) override
    {
        YT_VERIFY(IsPhysicalChunkType(TypeFromId(chunkId)));

        auto now = TInstant::Now();

        auto update = [&] (TEntry& entry) {
            entry.Promise = MakePromise(replicas);
            entry.Future = entry.Promise.ToFuture();
            entry.LastAccessTime = now;

            YT_LOG_DEBUG("Chunk replicas updated (ChunkId: %v, Replicas: %v, Revision: %x)",
                chunkId,
                MakeFormattableView(replicas.Replicas, TChunkReplicaAddressFormatter(NodeDirectory_)),
                replicas.Revision);
        };

        auto tryUpdate = [&] (TEntry& entry) {
            auto entryGuard = Guard(entry.Lock);

            auto oldRevision = NHydra::NullRevision;
            if (auto optionalExistingReplicas = entry.Future.TryGet()) {
                if (optionalExistingReplicas->IsOK()) {
                    oldRevision = optionalExistingReplicas->Value().Revision;
                }
            }

            if (oldRevision >= replicas.Revision) {
                return;
            }

            update(entry);
        };

        {
            auto mapGuard = ReaderGuard(EntriesLock_);
            auto it = Entries_.find(chunkId);
            if (it != Entries_.end()) {
                tryUpdate(*it->second);
                return;
            }
        }

        {
            auto mapGuard = WriterGuard(EntriesLock_);
            auto it = Entries_.find(chunkId);
            if (it == Entries_.end()) {
                it = EmplaceOrCrash(Entries_, chunkId, std::make_unique<TEntry>());
                update(*it->second);
            } else {
                tryUpdate(*it->second);
            }
        }
    }

    void RegisterReplicas(
        TChunkId chunkId,
        const TChunkReplicaWithMediumList& replicas) override
    {
        YT_VERIFY(IsPhysicalChunkType(TypeFromId(chunkId)));

        UpdateReplicas(
            chunkId,
            TAllyReplicasInfo{
                .Replicas = replicas,
                .Revision = 1 // must be larger than NullRevision
            });
    }

private:
    const TWeakPtr<NApi::NNative::IConnection> Connection_;
    const TChunkReplicaCacheConfigPtr Config_;
    const TNodeDirectoryPtr NodeDirectory_;
    const NLogging::TLogger Logger;

    const TPeriodicExecutorPtr ExpirationExecutor_;

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


    void OnChunksLocated(
        TCellTag cellTag,
        const std::vector<TChunkId>& chunkIds,
        const std::vector<TPromise<TAllyReplicasInfo>>& promises,
        const TChunkServiceProxy::TErrorOrRspLocateChunksPtr& rspOrError)
    {
        auto connection = Connection_.Lock();
        if (!connection) {
            return;
        }

        if (rspOrError.IsOK()) {
            YT_LOG_DEBUG("Chunks located (CellTag: %v, ChunkCount: %v)",
                cellTag,
                std::ssize(promises));

            const auto& rsp = rspOrError.Value();

            NodeDirectory_->MergeFrom(rsp->node_directory());

            for (int index = 0; index < std::ssize(chunkIds); ++index) {
                const auto& subresponse = rsp->subresponses(index);
                if (subresponse.missing()) {
                    promises[index].TrySet(TError(
                        NChunkClient::EErrorCode::NoSuchChunk,
                        "No such chunk %v",
                        chunkIds[index]));
                    continue;
                }
                auto replicas = subresponse.replicas_size() == 0
                    ? FromProto<TChunkReplicaList>(subresponse.legacy_replicas())
                    : TChunkReplicaWithMedium::ToChunkReplicas(FromProto<TChunkReplicaWithMediumList>(subresponse.replicas()));
                auto replicasInfo = TAllyReplicasInfo::FromChunkReplicas(replicas, rsp->revision());
                promises[index].TrySet(std::move(replicasInfo));
            }
        } else {
            YT_LOG_WARNING(rspOrError, "Error locating chunks (CellTag: %v)",
                cellTag);

            {
                auto mapGuard = WriterGuard(EntriesLock_);
                for (int index = 0; index < std::ssize(chunkIds); ++index) {
                    auto chunkId = chunkIds[index];
                    auto it = Entries_.find(chunkId);
                    if (it == Entries_.end()) {
                        continue;
                    }
                    Entries_.erase(it);
                }
            }

            auto error = TError(rspOrError);
            for (const auto& promise : promises) {
                promise.TrySet(error);
            }
        }
    }

    void OnExpirationSweep()
    {
        YT_LOG_DEBUG("Started expired chunk replica sweep");

        std::vector<TChunkId> expiredChunkIds;
        auto deadline = TInstant::Now() - Config_->ExpirationTime;
        int totalChunkCount;

        {
            auto mapGuard = ReaderGuard(EntriesLock_);
            totalChunkCount = std::ssize(Entries_);
            for (const auto& [chunkId, entry] : Entries_) {
                auto entryGuard = Guard(entry->Lock);
                if (entry->LastAccessTime < deadline) {
                    expiredChunkIds.push_back(chunkId);
                }
            }
        }

        if (!expiredChunkIds.empty()) {
            auto mapGuard = WriterGuard(EntriesLock_);
            for (auto chunkId : expiredChunkIds) {
                Entries_.erase(chunkId);
            }
        }

        YT_LOG_DEBUG("Finished expired chunk replica sweep (TotalChunkCount: %v, ExpiredChunkCount: %v)",
            totalChunkCount,
            expiredChunkIds.size());
    }
};

////////////////////////////////////////////////////////////////////////////////

IChunkReplicaCachePtr CreateChunkReplicaCache(NApi::NNative::IConnectionPtr connection)
{
    return New<TChunkReplicaCache>(std::move(connection));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
