#include "tablet_chunk_manager.h"

#include "config.h"
#include "helpers.h"
#include "hunk_storage_node.h"
#include "hunk_tablet.h"

#include <yt/yt/server/master/cell_master/automaton.h>
#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/config.h>

#include <yt/yt/server/master/table_server/table_node.h>

#include <yt/yt/server/master/chunk_server/chunk_manager.h>
#include <yt/yt/server/master/chunk_server/chunk_list.h>
#include <yt/yt/server/master/chunk_server/dynamic_store.h>
#include <yt/yt/server/master/chunk_server/helpers.h>
#include <yt/yt/server/master/chunk_server/chunk_tree_traverser.h>

#include <yt/yt/server/master/object_server/object_manager.h>

#include <yt/yt/server/lib/tablet_server/proto/tablet_manager.pb.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/session_id.h>

#include <yt/yt/ytlib/tablet_client/helpers.h>

#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/table_client/helpers.h>

#include <yt/yt/client/chunk_client/chunk_replica.h>

namespace NYT::NTabletServer {

using namespace NCellMaster;
using namespace NChunkServer;
using namespace NChunkClient;
using namespace NHydra;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NProfiling;
using namespace NTableClient;
using namespace NTableServer;
using namespace NTabletClient;
using namespace NTransactionClient;
using namespace NTabletClient;

using NChunkClient::NProto::TMiscExt;
using NTableClient::NProto::THunkChunkRefsExt;
using NTableClient::NProto::TBoundaryKeysExt;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TabletServerLogger;

////////////////////////////////////////////////////////////////////////////////

struct TProfilingCounters
{
    TProfilingCounters() = default;

    explicit TProfilingCounters(const TProfiler& profiler)
        : CopyChunkListIfSharedActionCount(profiler.Counter("/copy_chunk_list_if_shared/action_count"))
        , UpdateTabletStoresStoreCount(profiler.Counter("/update_tablet_stores/store_count"))
        , UpdateTabletStoresTime(profiler.TimeCounter("/update_tablet_stores/cumulative_time"))
        , CopyChunkListTime(profiler.TimeCounter("/copy_chunk_list_if_shared/cumulative_time"))
    { }

    const TCounter CopyChunkListIfSharedActionCount{};
    const TCounter UpdateTabletStoresStoreCount{};
    const TTimeCounter UpdateTabletStoresTime{};
    const TTimeCounter CopyChunkListTime{};
};

////////////////////////////////////////////////////////////////////////////////

class TTabletChunkManager
    : public ITabletChunkManager
    , public TMasterAutomatonPart
{
public:
    explicit TTabletChunkManager(TBootstrap* bootstrap)
        : TMasterAutomatonPart(bootstrap, EAutomatonThreadQueue::TabletManager)
    { }

    void CopyChunkListsIfShared(
        TTableNode* table,
        int firstTabletIndex,
        int lastTabletIndex,
        bool force = false) override
    {
        for (auto contentType : TEnumTraits<EChunkListContentType>::GetDomainValues()) {
            CopyChunkListIfShared(
                table,
                contentType,
                firstTabletIndex,
                lastTabletIndex,
                force);
        }
    }

    void CopyChunkListIfShared(
        TTableNode* table,
        EChunkListContentType contentType,
        int firstTabletIndex,
        int lastTabletIndex,
        bool force = false) override
    {
        TWallTimer timer;
        auto* counters = GetCounters({}, table);
        auto reportTimeGuard = Finally([&] {
            counters->CopyChunkListTime.Add(timer.GetElapsedTime());
        });

        int actionCount = 0;

        auto* oldRootChunkList = table->GetChunkList(contentType);
        auto& chunkLists = oldRootChunkList->Children();
        const auto& chunkManager = Bootstrap_->GetChunkManager();

        auto checkStatisticsMatch = [] (const TChunkTreeStatistics& lhs, TChunkTreeStatistics rhs) {
            rhs.ChunkListCount = lhs.ChunkListCount;
            rhs.Rank = lhs.Rank;
            return lhs == rhs;
        };

        if (oldRootChunkList->GetObjectRefCounter(/*flushUnrefs*/ true) > 1) {
            auto statistics = oldRootChunkList->Statistics();
            auto* newRootChunkList = chunkManager->CreateChunkList(oldRootChunkList->GetKind());
            chunkManager->AttachToChunkList(
                newRootChunkList,
                chunkLists.data(),
                chunkLists.data() + firstTabletIndex);

            for (int index = firstTabletIndex; index <= lastTabletIndex; ++index) {
                auto* newTabletChunkList = chunkManager->CloneTabletChunkList(chunkLists[index]->AsChunkList());
                chunkManager->AttachToChunkList(newRootChunkList, newTabletChunkList);

                actionCount += newTabletChunkList->Statistics().ChunkCount;
            }

            chunkManager->AttachToChunkList(
                newRootChunkList,
                chunkLists.data() + lastTabletIndex + 1,
                chunkLists.data() + chunkLists.size());

            actionCount += newRootChunkList->Children().size();

            // Replace root chunk list.
            table->SetChunkList(contentType, newRootChunkList);
            newRootChunkList->AddOwningNode(table);
            oldRootChunkList->RemoveOwningNode(table);
            if (!checkStatisticsMatch(newRootChunkList->Statistics(), statistics)) {
                YT_LOG_ALERT(
                    "Invalid new root chunk list statistics "
                    "(TableId: %v, ContentType: %v, NewRootChunkListStatistics: %v, Statistics: %v)",
                    table->GetId(),
                    contentType,
                    newRootChunkList->Statistics(),
                    statistics);
            }
        } else {
            auto statistics = oldRootChunkList->Statistics();

            for (int index = firstTabletIndex; index <= lastTabletIndex; ++index) {
                auto* oldTabletChunkList = chunkLists[index]->AsChunkList();
                if (force || oldTabletChunkList->GetObjectRefCounter(/*flushUnrefs*/ true) > 1) {
                    auto* newTabletChunkList = chunkManager->CloneTabletChunkList(oldTabletChunkList);
                    chunkManager->ReplaceChunkListChild(oldRootChunkList, index, newTabletChunkList);

                    actionCount += newTabletChunkList->Statistics().ChunkCount;

                    // ReplaceChunkListChild assumes that statistics are updated by caller.
                    // Here everything remains the same except for missing subtablet chunk lists.
                    int subtabletChunkListCount = oldTabletChunkList->Statistics().ChunkListCount - 1;
                    if (subtabletChunkListCount > 0) {
                        TChunkTreeStatistics delta{};
                        delta.ChunkListCount = -subtabletChunkListCount;
                        AccumulateUniqueAncestorsStatistics(newTabletChunkList, delta);
                    }
                }
            }

            if (!checkStatisticsMatch(oldRootChunkList->Statistics(), statistics)) {
                YT_LOG_ALERT(
                    "Invalid old root chunk list statistics "
                    "(TableId: %v, ContentType: %v, OldRootChunkListStatistics: %v, Statistics: %v)",
                    table->GetId(),
                    contentType,
                    oldRootChunkList->Statistics(),
                    statistics);
            }
        }

        if (actionCount > 0) {
            counters->CopyChunkListIfSharedActionCount.Increment(actionCount);
        }
    }

    void ReshardTable(
        TTableNode* table,
        int firstTabletIndex,
        int lastTabletIndex,
        int newTabletCount,
        const std::vector<TLegacyOwningKey>& oldPivotKeys,
        const std::vector<TLegacyOwningKey>& newPivotKeys,
        const THashSet<TStoreId>& oldEdenStoreIds) override
    {
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        const auto& objectManager = Bootstrap_->GetObjectManager();

        auto tablets = MakeMutableRange(table->MutableTablets());
        auto newTablets = tablets.Slice(firstTabletIndex, firstTabletIndex + newTabletCount);

        int oldTabletCount = lastTabletIndex - firstTabletIndex + 1;

        // Copy chunk tree if somebody holds a reference.
        CopyChunkListsIfShared(table, firstTabletIndex, lastTabletIndex);

        TChunkLists oldRootChunkLists;
        for (auto contentType : TEnumTraits<EChunkListContentType>::GetDomainValues()) {
            oldRootChunkLists[contentType] = table->GetChunkList(contentType);
        }

        TEnumIndexedVector<EChunkListContentType, std::vector<TChunkTree*>> newTabletChunkLists;
        for (auto& tabletChunkLists : newTabletChunkLists) {
            tabletChunkLists.reserve(newTabletCount);
        }

        TChunkLists newRootChunkLists;
        for (auto contentType : TEnumTraits<EChunkListContentType>::GetDomainValues()) {
            auto* chunkList = chunkManager->CreateChunkList(oldRootChunkLists[contentType]->GetKind());
            newRootChunkLists[contentType] = chunkList;
        }

        // Initialize new tablet chunk lists.
        if (table->IsPhysicallySorted()) {
            // This excludes hunk chunks.
            std::vector<TChunkTree*> chunksOrViews;

            // Chunk views that were created to fit chunks into old tablet range
            // and may later become useless after MergeChunkViewRanges.
            // We ref them after creation and unref at the end so they are
            // properly destroyed.
            std::vector<TChunkView*> temporarilyReferencedChunkViews;

            for (int index = firstTabletIndex; index <= lastTabletIndex; ++index) {
                auto* mainTabletChunkList = oldRootChunkLists[EChunkListContentType::Main]->Children()[index]->AsChunkList();
                auto tabletStores = EnumerateStoresInChunkTree(mainTabletChunkList);

                const auto& lowerPivot = oldPivotKeys[index - firstTabletIndex];
                const auto& upperPivot = oldPivotKeys[index - firstTabletIndex + 1];

                for (auto* chunkTree : tabletStores) {
                    if (chunkTree->GetType() == EObjectType::ChunkView) {
                        auto* chunkView = chunkTree->AsChunkView();
                        auto readRange = chunkView->GetCompleteReadRange();

                        // Check if chunk view fits into the old tablet completely.
                        // This might not be the case if the chunk view comes from bulk insert and has no read range.
                        if (readRange.LowerLimit().GetLegacyKey() < lowerPivot ||
                            upperPivot < readRange.UpperLimit().GetLegacyKey())
                        {
                            if (!chunkView->GetTransactionId()) {
                                YT_LOG_ALERT("Chunk view without transaction id is not fully inside its tablet "
                                    "(ChunkViewId: %v, UnderlyingTreeId: %v, "
                                    "EffectiveLowerLimit: %v, EffectiveUpperLimit: %v, "
                                    "PivotKey: %v, NextPivotKey: %v)",
                                    chunkView->GetId(),
                                    chunkView->GetUnderlyingTree()->GetId(),
                                    readRange.LowerLimit().GetLegacyKey(),
                                    readRange.UpperLimit().GetLegacyKey(),
                                    lowerPivot,
                                    upperPivot);
                            }

                            NChunkClient::TLegacyReadRange newReadRange;
                            if (readRange.LowerLimit().GetLegacyKey() < lowerPivot) {
                                newReadRange.LowerLimit().SetLegacyKey(lowerPivot);
                            }
                            if (upperPivot < readRange.UpperLimit().GetLegacyKey()) {
                                newReadRange.UpperLimit().SetLegacyKey(upperPivot);
                            }

                            auto* newChunkView = chunkManager->CreateChunkView(
                                chunkView,
                                TChunkViewModifier().WithReadRange(newReadRange));
                            objectManager->RefObject(newChunkView);
                            temporarilyReferencedChunkViews.push_back(newChunkView);

                            chunkTree = newChunkView;
                        }
                    }

                    chunksOrViews.push_back(chunkTree);
                }
            }

            SortUnique(chunksOrViews, TObjectIdComparer());

            auto keyColumnCount = table->GetSchema()->AsTableSchema()->GetKeyColumnCount();

            // Create new tablet chunk lists.
            for (int index = 0; index < newTabletCount; ++index) {
                auto* mainTabletChunkList = chunkManager->CreateChunkList(EChunkListKind::SortedDynamicTablet);
                mainTabletChunkList->SetPivotKey(newPivotKeys[index]);
                newTabletChunkLists[EChunkListContentType::Main].push_back(mainTabletChunkList);

                auto* hunkTabletChunkList = chunkManager->CreateChunkList(EChunkListKind::Hunk);
                newTabletChunkLists[EChunkListContentType::Hunk].push_back(hunkTabletChunkList);
            }

            // Move chunks or views from the resharded tablets to appropriate chunk lists.
            std::vector<std::vector<TChunkView*>> newTabletChildrenToBeMerged(newTablets.size());
            std::vector<std::vector<TChunkTree*>> newTabletHunkChunks(newTablets.size());
            std::vector<std::vector<TStoreId>> newEdenStoreIds(newTablets.size());

            for (auto* chunkOrView : chunksOrViews) {
                NChunkClient::TLegacyReadRange readRange;
                TChunk* chunk;
                if (chunkOrView->GetType() == EObjectType::ChunkView) {
                    auto* chunkView = chunkOrView->AsChunkView();
                    chunk = chunkView->GetUnderlyingTree()->AsChunk();
                    readRange = chunkView->GetCompleteReadRange();
                } else if (IsPhysicalChunkType(chunkOrView->GetType())) {
                    chunk = chunkOrView->AsChunk();
                    auto keyPair = GetChunkBoundaryKeys(chunk->ChunkMeta()->GetExtension<TBoundaryKeysExt>(), keyColumnCount);
                    readRange = {
                        NChunkClient::TLegacyReadLimit(keyPair.first),
                        NChunkClient::TLegacyReadLimit(GetKeySuccessor(keyPair.second))
                    };
                } else {
                    YT_ABORT();
                }

                auto referencedHunkChunks = GetReferencedHunkChunks(chunk);

                auto tabletsRange = GetIntersectingTablets(newTablets, readRange);
                for (auto it = tabletsRange.Begin(); it != tabletsRange.End(); ++it) {
                    auto* tablet = (*it)->As<TTablet>();
                    const auto& lowerPivot = tablet->GetPivotKey();
                    const auto& upperPivot = tablet->GetIndex() == std::ssize(tablets) - 1
                        ? MaxKey()
                        : tablets[tablet->GetIndex() + 1]->As<TTablet>()->GetPivotKey();
                    int relativeIndex = it - newTablets.Begin();

                    newTabletHunkChunks[relativeIndex].insert(
                        newTabletHunkChunks[relativeIndex].end(),
                        referencedHunkChunks.begin(),
                        referencedHunkChunks.end());

                    // Chunks or chunk views created directly from chunks may be attached to tablets as is.
                    // On the other hand, old chunk views may link to the same chunk and have adjacent ranges,
                    // so we handle them separately.
                    if (chunkOrView->GetType() == EObjectType::ChunkView) {
                        // Read range given by tablet's pivot keys will be enforced later.
                        newTabletChildrenToBeMerged[relativeIndex].push_back(chunkOrView->AsChunkView());
                    } else if (IsPhysicalChunkType(chunkOrView->GetType())) {
                        if (lowerPivot <= readRange.LowerLimit().GetLegacyKey() &&
                            readRange.UpperLimit().GetLegacyKey() <= upperPivot)
                        {
                            // Chunk fits into the tablet.
                            chunkManager->AttachToChunkList(
                                newTabletChunkLists[EChunkListContentType::Main][relativeIndex]->AsChunkList(),
                                chunk);
                            if (oldEdenStoreIds.contains(chunk->GetId())) {
                                newEdenStoreIds[relativeIndex].push_back(chunk->GetId());
                            }
                        } else {
                            // Chunk does not fit into the tablet, create chunk view.
                            NChunkClient::TLegacyReadRange newReadRange;
                            if (readRange.LowerLimit().GetLegacyKey() < lowerPivot) {
                                newReadRange.LowerLimit().SetLegacyKey(lowerPivot);
                            }
                            if (upperPivot < readRange.UpperLimit().GetLegacyKey()) {
                                newReadRange.UpperLimit().SetLegacyKey(upperPivot);
                            }
                            auto* newChunkView = chunkManager->CreateChunkView(
                                chunk,
                                TChunkViewModifier().WithReadRange(newReadRange));
                            chunkManager->AttachToChunkList(
                                newTabletChunkLists[EChunkListContentType::Main][relativeIndex]->AsChunkList(),
                                newChunkView);
                            if (oldEdenStoreIds.contains(chunk->GetId())) {
                                newEdenStoreIds[relativeIndex].push_back(newChunkView->GetId());
                            }
                        }
                    } else {
                        YT_ABORT();
                    }
                }
            }

            for (int relativeIndex = 0; relativeIndex < std::ssize(newTablets); ++relativeIndex) {
                auto* tablet = newTablets[relativeIndex]->As<TTablet>();
                const auto& lowerPivot = tablet->GetPivotKey();
                const auto& upperPivot = tablet->GetIndex() == std::ssize(tablets) - 1
                    ? MaxKey()
                    : tablets[tablet->GetIndex() + 1]->As<TTablet>()->GetPivotKey();

                std::vector<TChunkTree*> mergedChunkViews;
                try {
                    mergedChunkViews = MergeChunkViewRanges(newTabletChildrenToBeMerged[relativeIndex], lowerPivot, upperPivot);
                } catch (const std::exception& ex) {
                    YT_LOG_ALERT(ex, "Failed to merge chunk view ranges");
                }

                auto* newTabletChunkList = newTabletChunkLists[EChunkListContentType::Main][relativeIndex]->AsChunkList();
                chunkManager->AttachToChunkList(newTabletChunkList, mergedChunkViews);

                std::vector<TChunkTree*> hunkChunks;
                for (auto* chunkOrView : mergedChunkViews) {
                    if (oldEdenStoreIds.contains(chunkOrView->AsChunkView()->GetUnderlyingTree()->GetId())) {
                        newEdenStoreIds[relativeIndex].push_back(chunkOrView->GetId());
                    }
                }
            }

            for (int relativeIndex = 0; relativeIndex < std::ssize(newTablets); ++relativeIndex) {
                SetTabletEdenStoreIds(newTablets[relativeIndex]->As<TTablet>(), newEdenStoreIds[relativeIndex]);

                if (!newTabletHunkChunks[relativeIndex].empty()) {
                    SortUnique(newTabletHunkChunks[relativeIndex], TObjectIdComparer());
                    auto* hunkChunkList = newTabletChunkLists[EChunkListContentType::Hunk][relativeIndex]->AsChunkList();
                    chunkManager->AttachToChunkList(hunkChunkList, newTabletHunkChunks[relativeIndex]);
                }
            }

            for (auto* chunkView : temporarilyReferencedChunkViews) {
                if (objectManager->UnrefObject(chunkView) == 0) {
                    YT_LOG_DEBUG("Temporarily referenced chunk view dropped during reshard (ChunkViewId: %v)",
                        chunkView->GetId());
                }
            }
        } else {
            // If the number of tablets increases, just leave the new trailing ones empty.
            // If the number of tablets decreases, merge the original trailing ones.
            auto attachChunksToChunkList = [&] (TChunkList* chunkList, int firstTabletIndex, int lastTabletIndex) {
                std::vector<TChunk*> chunks;
                for (int index = firstTabletIndex; index <= lastTabletIndex; ++index) {
                    auto* mainTabletChunkList = oldRootChunkLists[EChunkListContentType::Main]->Children()[index]->AsChunkList();
                    EnumerateChunksInChunkTree(mainTabletChunkList, &chunks);
                }
                for (auto* chunk : chunks) {
                    chunkManager->AttachToChunkList(chunkList, chunk);
                }
            };
            for (int index = firstTabletIndex; index < firstTabletIndex + std::min(oldTabletCount, newTabletCount); ++index) {
                auto* oldChunkList = oldRootChunkLists[EChunkListContentType::Main]->Children()[index]->AsChunkList();
                auto* newChunkList = chunkManager->CloneTabletChunkList(oldChunkList);
                newTabletChunkLists[EChunkListContentType::Main].push_back(newChunkList);

                auto* newHunkChunkList = chunkManager->CreateChunkList(EChunkListKind::Hunk);
                newTabletChunkLists[EChunkListContentType::Hunk].push_back(newHunkChunkList);
            }
            if (oldTabletCount > newTabletCount) {
                auto* chunkList = newTabletChunkLists[EChunkListContentType::Main][newTabletCount - 1]->AsChunkList();
                attachChunksToChunkList(chunkList, firstTabletIndex + newTabletCount, lastTabletIndex);
            } else {
                for (int index = oldTabletCount; index < newTabletCount; ++index) {
                    newTabletChunkLists[EChunkListContentType::Main].push_back(chunkManager->CreateChunkList(EChunkListKind::OrderedDynamicTablet));
                    newTabletChunkLists[EChunkListContentType::Hunk].push_back(chunkManager->CreateChunkList(EChunkListKind::Hunk));
                }
            }

            for (auto contentType : TEnumTraits<EChunkListContentType>::GetDomainValues()) {
                YT_VERIFY(std::ssize(newTabletChunkLists[contentType]) == newTabletCount);
            }
        }

        // Update tablet chunk lists.
        for (auto contentType : TEnumTraits<EChunkListContentType>::GetDomainValues()) {
            const auto& oldTabletChunkLists = oldRootChunkLists[contentType]->Children();
            chunkManager->AttachToChunkList(
                newRootChunkLists[contentType],
                oldTabletChunkLists.data(),
                oldTabletChunkLists.data() + firstTabletIndex);
            chunkManager->AttachToChunkList(
                newRootChunkLists[contentType],
                newTabletChunkLists[contentType]);
            chunkManager->AttachToChunkList(
                newRootChunkLists[contentType],
                oldTabletChunkLists.data() + lastTabletIndex + 1,
                oldTabletChunkLists.data() + oldTabletChunkLists.size());
        }

        // Replace root chunk list.
        for (auto contentType : TEnumTraits<EChunkListContentType>::GetDomainValues()) {
            table->SetChunkList(contentType, newRootChunkLists[contentType]);
            newRootChunkLists[contentType]->AddOwningNode(table);
            oldRootChunkLists[contentType]->RemoveOwningNode(table);
        }
    }

    void ReshardHunkStorage(
        THunkStorageNode* hunkStorage,
        int firstTabletIndex,
        int lastTabletIndex,
        int newTabletCount) override
    {
        const auto& chunkManager = Bootstrap_->GetChunkManager();

        auto* oldRootChunkList = hunkStorage->GetChunkList();
        auto* newRootChunkList = chunkManager->CreateChunkList(EChunkListKind::HunkStorageRoot);
        std::vector<TChunkTree*> newTabletChunkLists;
        newTabletChunkLists.reserve(newTabletCount);
        for (int index = 0; index < newTabletCount; ++index) {
            auto* newTabletChunkList = chunkManager->CreateChunkList(EChunkListKind::HunkTablet);
            newTabletChunkLists.push_back(newTabletChunkList);
        }

        const auto& oldTabletChunkLists = oldRootChunkList->Children();

        chunkManager->AttachToChunkList(
            newRootChunkList,
            oldTabletChunkLists.data(),
            oldTabletChunkLists.data() + firstTabletIndex);

        chunkManager->AttachToChunkList(newRootChunkList, newTabletChunkLists);

        chunkManager->AttachToChunkList(
            newRootChunkList,
            oldTabletChunkLists.data() + lastTabletIndex + 1,
            oldTabletChunkLists.data() + oldTabletChunkLists.size());

        oldRootChunkList->RemoveOwningNode(hunkStorage);
        hunkStorage->SetChunkList(newRootChunkList);
        newRootChunkList->AddOwningNode(hunkStorage);
    }

    void PrepareUpdateTabletStores(
        TTablet* tablet,
        NProto::TReqUpdateTabletStores* request) override
    {
        auto validateStoreType = [&] (TObjectId id, TStringBuf action) {
            auto type = TypeFromId(id);
            if (!IsChunkTabletStoreType(type) &&
                !IsDynamicTabletStoreType(type) &&
                type != EObjectType::ChunkView)
            {
                THROW_ERROR_EXCEPTION("Cannot %v store %v of type %Qlv",
                    action,
                    id,
                    type);
            }
        };

        auto validateHunkChunkType = [&] (TChunkId id, TStringBuf action) {
            auto type = TypeFromId(id);
            if (!IsPhysicalChunkType(type)) {
                THROW_ERROR_EXCEPTION("Cannot %v hunk chunk %v of type %Qlv",
                    action,
                    id,
                    type);
            }
        };

        for (const auto& descriptor : request->stores_to_add()) {
            validateStoreType(FromProto<TObjectId>(descriptor.store_id()), "attach");
        }

        for (const auto& descriptor : request->stores_to_remove()) {
            validateStoreType(FromProto<TObjectId>(descriptor.store_id()), "detach");
        }

        for (const auto& descriptor : request->hunk_chunks_to_add()) {
            validateHunkChunkType(FromProto<TChunkId>(descriptor.chunk_id()), "attach");
        }

        for (const auto& descriptor : request->hunk_chunks_to_remove()) {
            validateHunkChunkType(FromProto<TChunkId>(descriptor.chunk_id()), "detach");
        }

        auto* table = tablet->GetTable();
        if (table->IsPhysicallySorted()) {
            PrepareUpdateTabletStoresSorted(tablet, request);
        } else {
            PrepareUpdateTabletStoresOrdered(tablet, request);
        }
    }

    TString CommitUpdateTabletStores(
        TTablet* tablet,
        NTransactionServer::TTransaction* transaction,
        NProto::TReqUpdateTabletStores* request,
        NTabletClient::ETabletStoresUpdateReason updateReason) override
    {
        auto* table = tablet->GetTable();

        TWallTimer timer;
        auto counters = GetCounters(updateReason, table);
        auto updateTime = Finally([&] {
            counters->UpdateTabletStoresTime.Add(timer.GetElapsedTime());
        });

        // Collect all changes first.
        const auto& chunkManager = Bootstrap_->GetChunkManager();

        // Dynamic stores are also possible.
        std::vector<TChunkTree*> chunksToAttach;
        i64 attachedRowCount = 0;
        auto lastCommitTimestamp = table->GetLastCommitTimestamp();

        TChunk* flushedChunk = nullptr;

        auto validateChunkAttach = [&] (TChunk* chunk) {
            if (!IsObjectAlive(chunk)) {
                YT_LOG_ALERT("Attempt to attach a zombie chunk (ChunkId: %v)",
                    chunk->GetId());
            }
            if (!IsJournalChunkType(chunk->GetType()) && chunk->HasParents()) {
                YT_LOG_ALERT("Attempt to attach a chunk that already has a parent (ChunkId: %v)",
                    chunk->GetId());
            }
        };

        for (const auto& descriptor : request->stores_to_add()) {
            auto storeId = FromProto<TStoreId>(descriptor.store_id());
            auto type = TypeFromId(storeId);
            if (IsChunkTabletStoreType(type)) {
                auto* chunk = chunkManager->GetChunkOrThrow(storeId);
                validateChunkAttach(chunk);
                auto miscExt = chunk->ChunkMeta()->FindExtension<TMiscExt>();
                if (miscExt && miscExt->has_max_timestamp()) {
                    lastCommitTimestamp = std::max(lastCommitTimestamp, static_cast<TTimestamp>(miscExt->max_timestamp()));
                }

                attachedRowCount += chunk->GetRowCount();
                chunksToAttach.push_back(chunk);
            } else if (IsDynamicTabletStoreType(type)) {
                if (IsDynamicStoreReadEnabled(table, GetDynamicConfig())) {
                    YT_LOG_ALERT("Attempt to attach dynamic store to a table "
                        "with readable dynamic stores (TableId: %v, TabletId: %v, StoreId: %v, Reason: %v)",
                        table->GetId(),
                        tablet->GetId(),
                        storeId,
                        updateReason);
                }
            } else {
                YT_ABORT();
            }
        }

        std::vector<TChunkTree*> hunkChunksToAttach;
        hunkChunksToAttach.reserve(request->hunk_chunks_to_add_size());
        for (const auto& descriptor : request->hunk_chunks_to_add()) {
            auto chunkId = FromProto<TChunkId>(descriptor.chunk_id());
            auto* chunk = chunkManager->GetChunkOrThrow(chunkId);
            validateChunkAttach(chunk);
            hunkChunksToAttach.push_back(chunk);
        }

        if (updateReason == ETabletStoresUpdateReason::Flush) {
            YT_VERIFY(request->stores_to_add_size() <= 1);
            if (request->stores_to_add_size() == 1) {
                flushedChunk = chunksToAttach[0]->AsChunk();
            }

            if (request->request_dynamic_store_id()) {
                auto storeId = ReplaceTypeInId(
                    transaction->GetId(),
                    table->IsPhysicallySorted()
                        ? EObjectType::SortedDynamicTabletStore
                        : EObjectType::OrderedDynamicTabletStore);
                auto* dynamicStore = CreateDynamicStore(tablet, storeId);
                chunksToAttach.push_back(dynamicStore);
                YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Dynamic store attached to tablet during flush "
                    "(TableId: %v, TabletId: %v, StoreId: %v)",
                    table->GetId(),
                    tablet->GetId(),
                    storeId);
            }
        }

        // Chunk views are also possible.
        std::vector<TChunkTree*> chunksOrViewsToDetach;
        i64 detachedRowCount = 0;
        bool flatteningRequired = false;
        for (const auto& descriptor : request->stores_to_remove()) {
            auto storeId = FromProto<TStoreId>(descriptor.store_id());
            if (IsChunkTabletStoreType(TypeFromId(storeId))) {
                auto* chunk = chunkManager->GetChunkOrThrow(storeId);
                detachedRowCount += chunk->GetRowCount();
                chunksOrViewsToDetach.push_back(chunk);
                flatteningRequired |= !CanUnambiguouslyDetachChild(tablet->GetChunkList(), chunk);
            } else if (TypeFromId(storeId) == EObjectType::ChunkView) {
                auto* chunkView = chunkManager->GetChunkViewOrThrow(storeId);
                auto* chunk = chunkView->GetUnderlyingTree()->AsChunk();
                detachedRowCount += chunk->GetRowCount();
                chunksOrViewsToDetach.push_back(chunkView);
                flatteningRequired |= !CanUnambiguouslyDetachChild(tablet->GetChunkList(), chunkView);
            } else if (IsDynamicTabletStoreType(TypeFromId(storeId))) {
                if (auto* dynamicStore = chunkManager->FindDynamicStore(storeId)) {
                    YT_VERIFY(updateReason == ETabletStoresUpdateReason::Flush);
                    dynamicStore->SetFlushedChunk(flushedChunk);
                    if (!table->IsSorted()) {
                        // NB: Dynamic stores at the end of the chunk list do not contribute to row count,
                        // so the logical row count of the chunk list is exactly the number of rows
                        // in all tablet chunks.
                        dynamicStore->SetTableRowIndex(tablet->GetChunkList()->Statistics().LogicalRowCount);
                    }
                    chunksOrViewsToDetach.push_back(dynamicStore);
                }
            } else {
                YT_ABORT();
            }
        }

        std::vector<TChunkTree*> hunkChunksToDetach;
        hunkChunksToDetach.reserve(request->hunk_chunks_to_remove_size());
        for (const auto& descriptor : request->hunk_chunks_to_remove()) {
            auto chunkId = FromProto<TStoreId>(descriptor.chunk_id());
            auto* chunk = chunkManager->GetChunkOrThrow(chunkId);
            hunkChunksToDetach.push_back(chunk);
        }

        // Update last commit timestamp.
        table->SetLastCommitTimestamp(lastCommitTimestamp);

        // Copy chunk trees if somebody holds a reference or if children cannot be detached unambiguously.
        CopyChunkListsIfShared(table, tablet->GetIndex(), tablet->GetIndex(), flatteningRequired);

        // Apply all requested changes.
        auto* tabletChunkList = tablet->GetChunkList();

        if (!table->IsPhysicallySorted() &&
            IsDynamicStoreReadEnabled(table, GetDynamicConfig()) &&
            updateReason == ETabletStoresUpdateReason::Flush)
        {
            // NB: Flushing ordered tablet requires putting a certain chunk in place of a certain dynamic store.

            const auto& children = tabletChunkList->Children();
            YT_VERIFY(!children.empty());

            auto* dynamicStoreToRemove = chunksOrViewsToDetach[0]->AsDynamicStore();
            int firstDynamicStoreIndex = GetFirstDynamicStoreIndex(tabletChunkList);
            YT_VERIFY(dynamicStoreToRemove == children[firstDynamicStoreIndex]);

            std::vector<TChunkTree*> allDynamicStores(
                children.begin() + firstDynamicStoreIndex,
                children.end());

            // +2 is due to that the accounting is not very precise at the node part.
            if (allDynamicStores.size() > NTabletNode::DynamicStoreCountLimit + 2) {
                YT_LOG_ALERT("Too many dynamic stores in ordered tablet chunk list "
                    "(TableId: %v, TabletId: %v, ChunkListId: %v, DynamicStoreCount: %v, "
                    "Limit: %v)",
                    table->GetId(),
                    tablet->GetId(),
                    tabletChunkList->GetId(),
                    allDynamicStores.size(),
                    NTabletNode::DynamicStoreCountLimit + 2);
            }

            chunkManager->DetachFromChunkList(
                tabletChunkList,
                allDynamicStores,
                EChunkDetachPolicy::OrderedTabletSuffix);

            if (flushedChunk) {
                chunkManager->AttachToChunkList(tabletChunkList, flushedChunk);
            }

            allDynamicStores.erase(allDynamicStores.begin());
            chunkManager->AttachToChunkList(tabletChunkList, allDynamicStores);

            if (request->request_dynamic_store_id()) {
                auto* dynamicStoreToAdd = chunksToAttach.back()->AsDynamicStore();
                chunkManager->AttachToChunkList(tabletChunkList, dynamicStoreToAdd);
            }
        } else {
            AttachChunksToTablet(tablet, chunksToAttach);
            DetachChunksFromTablet(
                tablet,
                chunksOrViewsToDetach,
                updateReason == ETabletStoresUpdateReason::Trim
                    ? EChunkDetachPolicy::OrderedTabletPrefix
                    : EChunkDetachPolicy::SortedTablet);
        }

        AttachChunksToTablet(tablet, hunkChunksToAttach);
        DetachChunksFromTablet(
            tablet,
            hunkChunksToDetach,
            table->IsPhysicallySorted()
                ? EChunkDetachPolicy::SortedTablet
                : EChunkDetachPolicy::OrderedTabletPrefix);

        // Unstage just attached chunks.
        for (auto* chunk : chunksToAttach) {
            if (IsChunkTabletStoreType(chunk->GetType())) {
                chunkManager->UnstageChunk(chunk->AsChunk());
            }
        }

        // Requisition update pursues two goals: updating resource usage and
        // setting requisitions to correct values. The latter is required both
        // for detached chunks (for obvious reasons) and attached chunks
        // (because the protocol doesn't allow for creating chunks with correct
        // requisitions from the start).
        for (auto* chunk : chunksToAttach) {
            chunkManager->ScheduleChunkRequisitionUpdate(chunk);
        }
        for (auto* chunk : chunksOrViewsToDetach) {
            chunkManager->ScheduleChunkRequisitionUpdate(chunk);
        }

        counters->UpdateTabletStoresStoreCount.Increment(chunksToAttach.size() + chunksOrViewsToDetach.size());

        return Format("AttachedChunkIds: %v, DetachedChunkOrViewIds: %v, "
            "AttachedHunkChunkIds: %v, DetachedHunkChunkIds: %v, "
            "AttachedRowCount: %v, DetachedRowCount: %v",
            MakeFormattableView(chunksToAttach, TObjectIdFormatter()),
            MakeFormattableView(chunksOrViewsToDetach, TObjectIdFormatter()),
            MakeFormattableView(hunkChunksToAttach, TObjectIdFormatter()),
            MakeFormattableView(hunkChunksToDetach, TObjectIdFormatter()),
            attachedRowCount,
            detachedRowCount);
    }

    TString CommitUpdateHunkTabletStores(
        THunkTablet* tablet,
        NProto::TReqUpdateHunkTabletStores* request) override
    {
        const auto& chunkManager = Bootstrap_->GetChunkManager();

        std::vector<TChunkTree*> chunksToAdd;
        chunksToAdd.reserve(request->stores_to_add_size());
        for (const auto& storeToAdd : request->stores_to_add()) {
            auto chunkId = FromProto<NChunkClient::TSessionId>(storeToAdd.session_id()).ChunkId;
            auto* chunk = chunkManager->FindChunk(chunkId);
            if (!IsObjectAlive(chunk)) {
                YT_LOG_ALERT(
                    "Requested to attach dead chunk to hunk tablet; ignored "
                    "(ChunkId: %v, HunkStorageId: %v, TableId: %v)",
                    chunk->GetId(),
                    tablet->GetId(),
                    tablet->GetOwner()->GetId());
                continue;
            }

            chunksToAdd.push_back(chunk);
        }

        AttachChunksToTablet(tablet, chunksToAdd);

        std::vector<TChunkTree*> chunksToRemove;
        chunksToRemove.reserve(request->stores_to_remove_size());
        for (const auto& storeToRemove : request->stores_to_remove()) {
            auto chunkId = FromProto<TChunkId>(storeToRemove.store_id());
            auto* chunk = chunkManager->FindChunk(chunkId);
            if (!IsObjectAlive(chunk)) {
                YT_LOG_ALERT(
                    "Requested to detach dead chunk from hunk tablet; ignored "
                    "(ChunkId: %v, TabletId: %v, HunkStorageId: %v)",
                    chunk->GetId(),
                    tablet->GetId(),
                    tablet->GetOwner()->GetId());
                continue;
            }

            chunksToRemove.push_back(chunk);
        }

        DetachChunksFromTablet(tablet, chunksToRemove, EChunkDetachPolicy::HunkTablet);

        std::vector<TChunk*> chunksToMarkSealable;
        for (const auto& chunkToMarkSealable : request->stores_to_mark_sealable()) {
            auto chunkId = FromProto<TChunkId>(chunkToMarkSealable.store_id());
            auto* chunk = chunkManager->GetChunk(chunkId);
            if (!IsObjectAlive(chunk)) {
                YT_LOG_ALERT(
                    "Requested to mark dead chunk as sealable; ignored "
                    "(ChunkId: %v, TabletId: %v, HunkStorageId: %v)",
                    chunk->GetId(),
                    tablet->GetId(),
                    tablet->GetOwner()->GetId());
                continue;
            }

            chunksToMarkSealable.push_back(chunk);
        }

        for (auto* chunk : chunksToMarkSealable) {
            chunk->SetSealable(true);
            chunkManager->ScheduleChunkSeal(chunk);
        }

        for (auto* chunk : chunksToAdd) {
            chunkManager->ScheduleChunkRequisitionUpdate(chunk);
        }
        for (auto* chunk : chunksToRemove) {
            chunkManager->ScheduleChunkRequisitionUpdate(chunk);
        }

        return Format("AddedChunkIds: %v, RemovedChunkIds: %v, MarkSealableChunkIds: %v)",
            MakeFormattableView(chunksToAdd, TObjectIdFormatter()),
            MakeFormattableView(chunksToRemove, TObjectIdFormatter()),
            MakeFormattableView(chunksToMarkSealable, TObjectIdFormatter()));
    }

    void MakeTableDynamic(NTableServer::TTableNode* table) override
    {
        auto* oldChunkList = table->GetChunkList();

        auto chunks = EnumerateChunksInChunkTree(oldChunkList);

        // Compute last commit timestamp.
        auto lastCommitTimestamp = MinTimestamp;
        for (auto* chunk : chunks) {
            auto miscExt = chunk->ChunkMeta()->FindExtension<TMiscExt>();
            if (miscExt && miscExt->has_max_timestamp()) {
                lastCommitTimestamp = std::max(lastCommitTimestamp, static_cast<TTimestamp>(miscExt->max_timestamp()));
            }
        }

        table->SetLastCommitTimestamp(lastCommitTimestamp);

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        auto* newChunkList = chunkManager->CreateChunkList(table->IsPhysicallySorted()
            ? EChunkListKind::SortedDynamicRoot
            : EChunkListKind::OrderedDynamicRoot);

        table->SetChunkList(newChunkList);
        newChunkList->AddOwningNode(table);

        auto* newHunkChunkList = chunkManager->CreateChunkList(EChunkListKind::HunkRoot);
        table->SetHunkChunkList(newHunkChunkList);
        newHunkChunkList->AddOwningNode(table);

        auto* tabletChunkList = chunkManager->CreateChunkList(table->IsPhysicallySorted()
            ? EChunkListKind::SortedDynamicTablet
            : EChunkListKind::OrderedDynamicTablet);
        if (table->IsPhysicallySorted()) {
            tabletChunkList->SetPivotKey(EmptyKey());
        }
        chunkManager->AttachToChunkList(newChunkList, tabletChunkList);

        std::vector<TChunkTree*> chunkTrees(chunks.begin(), chunks.end());
        chunkManager->AttachToChunkList(tabletChunkList, chunkTrees);

        auto* tabletHunkChunkList = chunkManager->CreateChunkList(EChunkListKind::Hunk);
        chunkManager->AttachToChunkList(newHunkChunkList, tabletHunkChunkList);

        oldChunkList->RemoveOwningNode(table);
    }

    void MakeTableStatic(NTableServer::TTableNode* table) override
    {
        auto* oldChunkList = table->GetChunkList();
        auto* oldHunkChunkList = table->GetHunkChunkList();

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        auto* newChunkList = chunkManager->CreateChunkList(EChunkListKind::Static);

        table->SetChunkList(newChunkList);
        newChunkList->AddOwningNode(table);

        table->SetHunkChunkList(nullptr);

        auto chunks = EnumerateChunksInChunkTree(oldChunkList);
        chunkManager->AttachToChunkList(newChunkList, std::vector<TChunkTree*>{chunks.begin(), chunks.end()});

        YT_VERIFY(EnumerateChunksInChunkTree(oldHunkChunkList).empty());

        oldChunkList->RemoveOwningNode(table);
        oldHunkChunkList->RemoveOwningNode(table);
    }

    void SetTabletEdenStoreIds(TTablet* tablet, std::vector<TStoreId> edenStoreIds) override
    {
        i64 masterMemoryUsageDelta = -static_cast<i64>(tablet->EdenStoreIds().size() * sizeof(TStoreId));
        if (edenStoreIds.size() <= EdenStoreIdsSizeLimit) {
            tablet->EdenStoreIds() = std::move(edenStoreIds);
            tablet->EdenStoreIds().shrink_to_fit();
        } else {
            tablet->EdenStoreIds() = {};
        }
        masterMemoryUsageDelta += tablet->EdenStoreIds().size() * sizeof(TStoreId);

        auto* table = tablet->GetTable();
        table->SetTabletMasterMemoryUsage(
            table->GetTabletMasterMemoryUsage() + masterMemoryUsageDelta);

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        securityManager->UpdateMasterMemoryUsage(table);
    }

    void DetachChunksFromTablet(
        TTabletBase* tablet,
        const std::vector<TChunkTree*>& chunkTrees,
        EChunkDetachPolicy policy) override
    {
        const auto& chunkManager = Bootstrap_->GetChunkManager();

        if (policy == EChunkDetachPolicy::OrderedTabletPrefix ||
            policy == EChunkDetachPolicy::OrderedTabletSuffix)
        {
            chunkManager->DetachFromChunkList(tablet->GetChunkList(), chunkTrees, policy);
            return;
        }

        YT_VERIFY(
            policy == EChunkDetachPolicy::SortedTablet ||
            policy == EChunkDetachPolicy::HunkTablet);

        // Ensure deteministic ordering of keys.
        std::map<TChunkList*, std::vector<TChunkTree*>, TObjectIdComparer> childrenByParent;
        for (auto* child : chunkTrees) {
            auto* parent = GetTabletChildParent(tablet, child);
            YT_VERIFY(HasParent(child, parent));
            childrenByParent[parent].push_back(child);
        }

        for (const auto& [parent, children] : childrenByParent) {
            chunkManager->DetachFromChunkList(parent, children, policy);
            PruneEmptySubtabletChunkList(parent);
        }
    }

    void WrapWithBackupChunkViews(TTablet* tablet, TTimestamp maxClipTimestamp) override
    {
        YT_VERIFY(tablet->GetState() == ETabletState::Unmounted);

        if (!maxClipTimestamp) {
            YT_LOG_ALERT(
                "Attempted to clip backup table by null timestamp (TableId: %v, TabletId: %v)",
                tablet->GetTable()->GetId(),
                tablet->GetId());
        }

        bool needFlatten = false;
        auto* chunkList = tablet->GetChunkList();
        for (const auto* child : chunkList->Children()) {
            if (child->GetType() == EObjectType::ChunkList) {
                needFlatten = true;
                break;
            }
        }

        auto* table = tablet->GetTable();
        CopyChunkListsIfShared(table, tablet->GetIndex(), tablet->GetIndex(), needFlatten);

        auto oldStatistics = tablet->GetTabletStatistics();
        table->DiscountTabletStatistics(oldStatistics);

        chunkList = tablet->GetChunkList();
        std::vector<TChunkTree*> storesToDetach;
        std::vector<TChunkTree*> storesToAttach;

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        const auto& transactionManager = Bootstrap_->GetTransactionManager();

        for (auto* store : chunkList->Children()) {
            TTimestamp minTimestamp = MinTimestamp;
            TTimestamp maxTimestamp = MaxTimestamp;

            auto takeTimestampsFromChunk = [&] (const TChunk* chunk) {
                auto miscExt = chunk->ChunkMeta()->FindExtension<TMiscExt>();
                if (miscExt) {
                    if (miscExt->has_min_timestamp()) {
                        minTimestamp = miscExt->min_timestamp();
                    }
                    if (miscExt->has_max_timestamp()) {
                        maxTimestamp = miscExt->max_timestamp();
                    }
                }
            };

            if (IsChunkTabletStoreType(store->GetType())) {
                takeTimestampsFromChunk(store->AsChunk());
            } else if (store->GetType() == EObjectType::ChunkView) {
                auto* chunkView = store->AsChunkView();

                if (auto transactionId = chunkView->GetTransactionId()) {
                    auto overrideTimestamp = transactionManager->GetTimestampHolderTimestamp(transactionId);
                    minTimestamp = overrideTimestamp;
                    maxTimestamp = overrideTimestamp;
                } else {
                    auto* underlyingTree = chunkView->GetUnderlyingTree();
                    YT_VERIFY(IsChunkTabletStoreType(underlyingTree->GetType()));
                    takeTimestampsFromChunk(underlyingTree->AsChunk());
                }
            }

            if (maxTimestamp <= maxClipTimestamp) {
                continue;
            }

            storesToDetach.push_back(store);

            if (IsDynamicTabletStoreType(store->GetType()) &&
                !tablet->BackupCutoffDescriptor()->DynamicStoreIdsToKeep.contains(store->GetId()))
            {
                continue;
            }

            if (minTimestamp <= maxClipTimestamp) {
                auto* wrappedStore = chunkManager->CreateChunkView(
                    store,
                    TChunkViewModifier().WithMaxClipTimestamp(maxClipTimestamp));
                storesToAttach.push_back(wrappedStore);
            }
        }

        chunkManager->DetachFromChunkList(chunkList, storesToDetach, EChunkDetachPolicy::SortedTablet);
        chunkManager->AttachToChunkList(chunkList, storesToAttach);

        auto newStatistics = tablet->GetTabletStatistics();
        table->AccountTabletStatistics(newStatistics);
    }

    TError PromoteFlushedDynamicStores(TTablet* tablet) override
    {
        if (auto error = CheckAllDynamicStoresFlushed(tablet); !error.IsOK()) {
            return error;
        }

        YT_VERIFY(tablet->GetState() == ETabletState::Unmounted);

        auto* table = tablet->GetTable();
        CopyChunkListsIfShared(table, tablet->GetIndex(), tablet->GetIndex());
        auto* chunkList = tablet->GetChunkList();

        auto oldStatistics = tablet->GetTabletStatistics();
        table->DiscountTabletStatistics(oldStatistics);

        std::vector<TChunkTree*> storesToDetach;
        std::vector<TChunkTree*> storesToAttach;

        const auto& chunkManager = Bootstrap_->GetChunkManager();

        for (auto* store : chunkList->Children()) {
            if (store->GetType() == EObjectType::ChunkView) {
                auto* chunkView = store->AsChunkView();

                auto* underlyingTree = chunkView->GetUnderlyingTree();
                if (!IsDynamicTabletStoreType(underlyingTree->GetType())) {
                    continue;
                }

                storesToDetach.push_back(chunkView);

                auto* dynamicStore = underlyingTree->AsDynamicStore();
                YT_VERIFY(dynamicStore->IsFlushed());
                auto* chunk = dynamicStore->GetFlushedChunk();

                if (chunk) {
                    // FIXME(ifsmirnov): chunk view is not always needed, check
                    // chunk min/max timestaps.
                    auto* wrappedStore = chunkManager->CreateChunkView(
                        chunk,
                        chunkView->Modifier());
                    storesToAttach.push_back(wrappedStore);
                }
            } else if (IsDynamicTabletStoreType(store->GetType())) {
                auto* dynamicStore = store->AsDynamicStore();
                YT_VERIFY(dynamicStore->IsFlushed());
                auto* chunk = dynamicStore->GetFlushedChunk();
                if (chunk) {
                    storesToAttach.push_back(chunk);
                }
                storesToDetach.push_back(dynamicStore);
            }
        }

        std::vector<TChunkTree*> hunkChunksToAttach;
        auto attachHunkChunks = [&] (TChunk* chunk) {
            auto hunkChunks = GetReferencedHunkChunks(chunk);
            if (!hunkChunks.empty()) {
                YT_VERIFY(std::ssize(hunkChunks) == 1);
                hunkChunksToAttach.push_back(hunkChunks[0]);
            }
        };

        for (auto* store : storesToAttach) {
            switch (store->GetType()) {
                case EObjectType::Chunk:
                case EObjectType::ErasureChunk: {
                    attachHunkChunks(store->AsChunk());
                    break;
                }

                case EObjectType::ChunkView: {
                    auto* chunkView = store->AsChunkView();
                    auto* underlyingTree = chunkView->GetUnderlyingTree();
                    YT_VERIFY(
                        underlyingTree->GetType() == EObjectType::Chunk ||
                        underlyingTree->GetType() == EObjectType::ErasureChunk);
                    attachHunkChunks(underlyingTree->AsChunk());
                    break;
                }

                default:
                    YT_ABORT();
            }
        }

        chunkManager->DetachFromChunkList(
            chunkList,
            storesToDetach,
            table->IsPhysicallySorted()
                ? EChunkDetachPolicy::SortedTablet
                : EChunkDetachPolicy::OrderedTabletSuffix);
        chunkManager->AttachToChunkList(chunkList, storesToAttach);

        if (!hunkChunksToAttach.empty()) {
            auto* hunkChunkList = tablet->GetHunkChunkList();
            chunkManager->AttachToChunkList(hunkChunkList, hunkChunksToAttach);
        }

        auto newStatistics = tablet->GetTabletStatistics();
        table->AccountTabletStatistics(newStatistics);

        return {};
    }

    TError ApplyBackupCutoff(TTablet* tablet) override
    {
        if (!tablet->BackupCutoffDescriptor()) {
            return {};
        }

        auto backupMode = tablet->GetTable()->GetBackupMode();

        TError error;

        switch (backupMode) {
            case EBackupMode::OrderedStrongCommitOrdering:
            case EBackupMode::OrderedExact:
            case EBackupMode::OrderedAtLeast:
            case EBackupMode::OrderedAtMost:
            case EBackupMode::ReplicatedSorted:
                error = ApplyRowIndexBackupCutoff(tablet);
                break;

            case EBackupMode::SortedAsyncReplica:
                ApplyDynamicStoreListBackupCutoff(tablet);
                break;

            default:
                YT_ABORT();
        }

        return error;
    }

    TDynamicStore* CreateDynamicStore(TTablet* tablet, TDynamicStoreId hintId = NullObjectId) override
    {
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        auto id = GenerateDynamicStoreId(tablet, hintId);
        return chunkManager->CreateDynamicStore(id, tablet);
    }

private:
    using TProfilerKey = std::tuple<std::optional<ETabletStoresUpdateReason>, TString, bool>;
    THashMap<TProfilerKey, TProfilingCounters> Counters_;

    TProfilingCounters* GetCounters(std::optional<ETabletStoresUpdateReason> reason, TTabletOwnerBase* owner)
    {
        static TProfilingCounters nullCounters;
        if (IsRecovery()) {
            return &nullCounters;
        }

        // TODO(gritukan)
        if (!IsTableType(owner->GetType())) {
            return &nullCounters;
        }

        auto* table = owner->As<TTableNode>();
        const auto& cellBundle = table->TabletCellBundle();
        if (!cellBundle) {
            return &nullCounters;
        }

        if (!IsObjectAlive(table)) {
            return &nullCounters;
        }

        TProfilerKey key{reason, cellBundle->GetName(), table->IsPhysicallySorted()};
        auto it = Counters_.find(key);
        if (it != Counters_.end()) {
            return &it->second;
        }

        auto profiler = TabletServerProfiler
            .WithSparse()
            .WithTag("tablet_cell_bundle", std::get<TString>(key))
            .WithTag("table_type", table->IsPhysicallySorted() ? "sorted" : "ordered");

        if (reason) {
            profiler = profiler.WithTag("update_reason", FormatEnum(*reason));
        }

        it = Counters_.emplace(
            key,
            TProfilingCounters{profiler}).first;
        return &it->second;
    }

    const TDynamicTabletManagerConfigPtr& GetDynamicConfig() const
    {
        return Bootstrap_->GetConfigManager()->GetConfig()->TabletManager;
    }

    TError CheckAllDynamicStoresFlushed(TTablet* tablet)
    {
        auto makeError = [&] (const TDynamicStore* dynamicStore) {
            const auto* originalTablet = dynamicStore->GetTablet();
            const TString& originalTablePath = IsObjectAlive(originalTablet) && IsObjectAlive(originalTablet->GetTable())
                ? originalTablet->GetTable()->GetMountPath()
                : "";
            return TError("Cannot restore table from backup since "
                "dynamic store %v in tablet %v is not flushed",
                dynamicStore->GetId(),
                tablet->GetId())
                << TErrorAttribute("original_table_path", originalTablePath)
                << TErrorAttribute("table_id", tablet->GetTable()->GetId());
        };

        auto children = EnumerateStoresInChunkTree(tablet->GetChunkList());

        for (const auto* child : children) {
            if (child->GetType() == EObjectType::ChunkView) {
                const auto* underlyingTree = child->AsChunkView()->GetUnderlyingTree();
                if (IsDynamicTabletStoreType(underlyingTree->GetType()) &&
                    !underlyingTree->AsDynamicStore()->IsFlushed())
                {
                    return makeError(underlyingTree->AsDynamicStore());
                }
            } else if (IsDynamicTabletStoreType(child->GetType())) {
                const auto* dynamicStore = child->AsDynamicStore();
                if (!dynamicStore->IsFlushed()) {
                    return makeError(dynamicStore);
                }
            }
        }

        return {};
    }

    TError ApplyRowIndexBackupCutoff(TTablet* tablet)
    {
        auto* chunkList = tablet->GetChunkList();
        YT_VERIFY(chunkList->GetKind() == EChunkListKind::OrderedDynamicTablet);

        const auto& descriptor = *tablet->BackupCutoffDescriptor();

        if (tablet->GetTrimmedRowCount() > descriptor.CutoffRowIndex) {
            return TError(
                "Cannot backup ordered tablet %v since it is trimmed "
                "beyond cutoff row index",
                tablet->GetId())
                << TErrorAttribute("tablet_id", tablet->GetId())
                << TErrorAttribute("table_id", tablet->GetTable()->GetId())
                << TErrorAttribute("trimmed_row_count", tablet->GetTrimmedRowCount())
                << TErrorAttribute("cutoff_row_index", descriptor.CutoffRowIndex);
        }

        if (!descriptor.NextDynamicStoreId) {
            // Tablet was not mounted when the descriptor was generated by the node.
            // Either it is fully flushed, then this check succeeds; or it is is not
            // (so the descriptor was generated during freeze/unmount workflow) and we
            // have to abort the backup if some of the stores are not flushed.
            for (const auto* child : tablet->GetChunkList()->Children()) {
                if (child && IsDynamicTabletStoreType(child->GetType())) {
                    const auto* dynamicStore = child->AsDynamicStore();
                    if (!dynamicStore->IsFlushed()) {
                        return TError(
                            "Cannot backup ordered tablet %v since it is not fully flushed "
                            "and its origin was not mounted during the backup",
                            tablet->GetId())
                            << TErrorAttribute("tablet_id", tablet->GetId())
                            << TErrorAttribute("table_id", tablet->GetTable()->GetId());
                    }
                }
            }
        }

        // CopyChunkListsIfShared must be done before cutoff chunk index is calculated
        // because it omits trimmed chunks and thus may shift chunk indexes.
        CopyChunkListsIfShared(tablet->GetTable(), tablet->GetIndex(), tablet->GetIndex());
        chunkList = tablet->GetChunkList();

        int cutoffChildIndex = 0;
        const auto& children = chunkList->Children();
        const auto& statistics = chunkList->CumulativeStatistics();

        auto wrapInternalErrorAndLog = [&] (TError innerError) {
            innerError = innerError
                << TErrorAttribute("table_id", tablet->GetTable()->GetId())
                << TErrorAttribute("tablet_id", tablet->GetId())
                << TErrorAttribute("cutoff_row_index", descriptor.CutoffRowIndex)
                << TErrorAttribute("next_dynamic_store_id", descriptor.NextDynamicStoreId)
                << TErrorAttribute("cutoff_child_index", cutoffChildIndex);
            auto error = TError("Cannot backup ordered tablet due to an internal error")
                << innerError;
            YT_LOG_ALERT(error, "Failed to perform backup cutoff");
            return error;
        };

        bool hitDynamicStore = false;

        while (cutoffChildIndex < ssize(children)) {
            i64 cumulativeRowCount = statistics.GetPreviousSum(cutoffChildIndex).RowCount;
            const auto* child = children[cutoffChildIndex];

            if (child && child->GetId() == descriptor.NextDynamicStoreId) {
                if (cumulativeRowCount > descriptor.CutoffRowIndex) {
                    auto error = TError("Cumulative row count at the cutoff dynamic store "
                        "is greater than expected")
                        << TErrorAttribute("cumulative_row_count", cumulativeRowCount);
                    return wrapInternalErrorAndLog(error);
                }

                hitDynamicStore = true;
                break;
            }

            if (cumulativeRowCount > descriptor.CutoffRowIndex) {
                auto error = TError("Cumulative row count exceeded cutoff row index")
                    << TErrorAttribute("cumulative_row_count", cumulativeRowCount);
                return wrapInternalErrorAndLog(error);
            }

            if (cumulativeRowCount == descriptor.CutoffRowIndex) {
                break;
            }

            ++cutoffChildIndex;
        }

        if (statistics.GetPreviousSum(cutoffChildIndex).RowCount != descriptor.CutoffRowIndex &&
            !hitDynamicStore)
        {
            auto error = TError("Row count at final cutoff child index does not match cutoff row index")
                << TErrorAttribute("cumulative_row_count", statistics.GetPreviousSum(cutoffChildIndex).RowCount);
            return wrapInternalErrorAndLog(error);
        }

        if (cutoffChildIndex == ssize(chunkList->Children())) {
            return {};
        }

        auto oldStatistics = tablet->GetTabletStatistics();
        auto* table = tablet->GetTable();
        table->DiscountTabletStatistics(oldStatistics);

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        std::vector<TChunkTree*> childrenToDetach(
            chunkList->Children().data() + cutoffChildIndex,
            chunkList->Children().data() + ssize(children));
        chunkManager->DetachFromChunkList(
            chunkList,
            childrenToDetach,
            EChunkDetachPolicy::OrderedTabletSuffix);

        auto newStatistics = tablet->GetTabletStatistics();
        table->AccountTabletStatistics(newStatistics);

        return {};
    }

    void ApplyDynamicStoreListBackupCutoff(TTablet* tablet)
    {
        auto* chunkList = tablet->GetChunkList();
        YT_VERIFY(chunkList->GetKind() == EChunkListKind::SortedDynamicTablet);

        const auto& descriptor = *tablet->BackupCutoffDescriptor();

        std::vector<TChunkTree*> storesToDetach;
        // NB: cannot use tablet->DynamicStores() since dynamic stores in the chunk list
        // in fact belong to the other tablet and are not linked with this one.
        for (auto* child : EnumerateStoresInChunkTree(tablet->GetChunkList())) {
            if (child->GetType() != EObjectType::SortedDynamicTabletStore) {
                continue;
            }
            if (!descriptor.DynamicStoreIdsToKeep.contains(child->GetId())) {
                storesToDetach.push_back(child);
            }
        }

        if (storesToDetach.empty()) {
            return;
        }

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(),
            "Detaching unneeded dynamic stores from tablet after backup "
            "(TabletId: %v, DynamicStoreIds: %v)",
            tablet->GetId(),
            MakeFormattableView(
                storesToDetach,
                TObjectIdFormatter{}));

        CopyChunkListsIfShared(tablet->GetTable(), tablet->GetIndex(), tablet->GetIndex());
        chunkList = tablet->GetChunkList();

        auto oldStatistics = tablet->GetTabletStatistics();
        auto* table = tablet->GetTable();
        table->DiscountTabletStatistics(oldStatistics);

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        chunkManager->DetachFromChunkList(
            chunkList,
            storesToDetach,
            EChunkDetachPolicy::SortedTablet);

        auto newStatistics = tablet->GetTabletStatistics();
        table->AccountTabletStatistics(newStatistics);
    }

    TDynamicStoreId GenerateDynamicStoreId(const TTablet* tablet, TDynamicStoreId hintId = NullObjectId) const
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto type = tablet->GetTable()->IsPhysicallySorted()
            ? EObjectType::SortedDynamicTabletStore
            : EObjectType::OrderedDynamicTabletStore;
        return objectManager->GenerateId(type, hintId);
    }

    TChunkList* GetTabletChildParent(TTabletBase* tablet, TChunkTree* child)
    {
        if (IsHunkChunk(tablet, child)) {
            return tablet->GetHunkChunkList();
        } else {
            if (GetParentCount(child) == 1) {
                auto* parent = GetUniqueParent(child);
                YT_VERIFY(parent->GetType() == EObjectType::ChunkList);
                return parent;
            }
            return tablet->GetChunkList();
        }
    }

    void PruneEmptySubtabletChunkList(TChunkList* chunkList)
    {
        while (chunkList->GetKind() == EChunkListKind::SortedDynamicSubtablet && chunkList->Children().empty()) {
            auto* parent = GetUniqueParent(chunkList);
            const auto& chunkManager = Bootstrap_->GetChunkManager();
            chunkManager->DetachFromChunkList(parent, chunkList, EChunkDetachPolicy::SortedTablet);
            chunkList = parent;
        }
    }

    std::vector<TChunk*> GetReferencedHunkChunks(TChunk* storeChunk)
    {
        auto hunkRefsExt = storeChunk->ChunkMeta()->FindExtension<THunkChunkRefsExt>();
        if (!hunkRefsExt) {
            return {};
        }

        std::vector<TChunk*> hunkChunks;

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        for (const auto& protoRef : hunkRefsExt->refs()) {
            auto hunkChunkId = FromProto<TChunkId>(protoRef.chunk_id());
            auto* hunkChunk = chunkManager->FindChunk(hunkChunkId);
            if (!IsObjectAlive(hunkChunk)) {
                YT_LOG_ALERT("Store references a non-existing hunk chunk (StoreId: %v, HunkChunkId: %v)",
                    storeChunk->GetId(),
                    hunkChunkId);
                continue;
            }
            hunkChunks.push_back(hunkChunk);
        }

        return hunkChunks;
    }

    static TMutableRange<TTabletBase*> GetIntersectingTablets(
        TMutableRange<TTabletBase*> tablets,
        const TLegacyReadRange& readRange)
    {
        YT_VERIFY(readRange.LowerLimit().HasLegacyKey());
        YT_VERIFY(readRange.UpperLimit().HasLegacyKey());
        const auto& minKey = readRange.LowerLimit().GetLegacyKey();
        const auto& maxKey = readRange.UpperLimit().GetLegacyKey();

        auto beginIt = std::upper_bound(
            tablets.begin(),
            tablets.end(),
            minKey,
            [] (const TLegacyOwningKey& key, const TTabletBase* tablet) {
                return key < tablet->As<TTablet>()->GetPivotKey();
            });

        if (beginIt != tablets.begin()) {
            --beginIt;
        }

        auto endIt = beginIt;
        while (endIt != tablets.end() && maxKey > (*endIt)->As<TTablet>()->GetPivotKey()) {
            ++endIt;
        }

        return {beginIt, endIt};
    }

    // If there are several otherwise identical chunk views with adjacent read ranges
    // we merge them into one chunk view with the joint range.
    std::vector<TChunkTree*> MergeChunkViewRanges(
        std::vector<TChunkView*> chunkViews,
        const TLegacyOwningKey& lowerPivot,
        const TLegacyOwningKey& upperPivot)
    {
        auto mergeResults = MergeAdjacentChunkViewRanges(std::move(chunkViews));
        std::vector<TChunkTree*> result;

        const auto& chunkManager = Bootstrap_->GetChunkManager();

        for (const auto& mergeResult : mergeResults) {
            auto* firstChunkView = mergeResult.FirstChunkView;
            auto* lastChunkView = mergeResult.LastChunkView;
            const auto& lowerLimit = firstChunkView->ReadRange().LowerLimit().HasLegacyKey()
                ? firstChunkView->ReadRange().LowerLimit().GetLegacyKey()
                : EmptyKey();
            const auto& upperLimit = lastChunkView->ReadRange().UpperLimit().HasLegacyKey()
                ? lastChunkView->ReadRange().UpperLimit().GetLegacyKey()
                : MaxKey();

            if (firstChunkView == lastChunkView &&
                lowerPivot <= lowerLimit &&
                upperLimit <= upperPivot)
            {
                result.push_back(firstChunkView);
                continue;
            } else {
                NChunkClient::TLegacyReadRange readRange;
                const auto& adjustedLower = std::max(lowerLimit, lowerPivot);
                const auto& adjustedUpper = std::min(upperLimit, upperPivot);
                YT_VERIFY(adjustedLower < adjustedUpper);
                if (adjustedLower != EmptyKey()) {
                    readRange.LowerLimit().SetLegacyKey(adjustedLower);
                }
                if (adjustedUpper != MaxKey()) {
                    readRange.UpperLimit().SetLegacyKey(adjustedUpper);
                }
                result.push_back(chunkManager->CloneChunkView(firstChunkView, readRange));
            }
        }

        return result;
    }

    void ValidateTabletContainsStore(const TTablet* tablet, TChunkTree* const store)
    {
        const auto* tabletChunkList = tablet->GetChunkList();

        // Fast path: the store belongs to the tablet directly.
        if (tabletChunkList->ChildToIndex().contains(store)) {
            return;
        }

        auto onParent = [&] (TChunkTree* parent) {
            if (parent->GetType() != EObjectType::ChunkList) {
                return false;
            }
            auto* chunkList = parent->AsChunkList();
            if (chunkList->GetKind() != EChunkListKind::SortedDynamicSubtablet) {
                return false;
            }
            if (tabletChunkList->ChildToIndex().contains(chunkList)) {
                return true;
            }
            return false;
        };

        // NB: tablet chunk list has rank of at most 2, so it suffices to check only
        // one intermediate chunk list between store and tablet.
        if (IsChunkTabletStoreType(store->GetType())) {
            for (auto& [parent, multiplicity] : store->AsChunk()->Parents()) {
                if (onParent(parent)) {
                    return;
                }
            }
        } else if (store->GetType() == EObjectType::ChunkView) {
            for (auto* parent : store->AsChunkView()->Parents()) {
                if (onParent(parent)) {
                    return;
                }
            }
        } else if (IsDynamicTabletStoreType(store->GetType())) {
            for (auto* parent : store->AsDynamicStore()->Parents()) {
                if (onParent(parent)) {
                    return;
                }
            }
        }

        THROW_ERROR_EXCEPTION("Store %v does not belong to tablet %v",
            store->GetId(),
            tablet->GetId());
    }

    static int GetFirstDynamicStoreIndex(const TChunkList* chunkList)
    {
        YT_VERIFY(chunkList->GetKind() == EChunkListKind::OrderedDynamicTablet);

        const auto& children = chunkList->Children();
        int firstDynamicStoreIndex = static_cast<int>(children.size()) - 1;
        YT_VERIFY(IsDynamicTabletStoreType(children[firstDynamicStoreIndex]->GetType()));
        while (firstDynamicStoreIndex > chunkList->GetTrimmedChildCount() &&
            IsDynamicTabletStoreType(children[firstDynamicStoreIndex - 1]->GetType()))
        {
            --firstDynamicStoreIndex;
        }

        return firstDynamicStoreIndex;
    }

    void PrepareUpdateTabletStoresSorted(
        TTablet* tablet,
        NProto::TReqUpdateTabletStores* request)
    {
        const auto* table = tablet->GetTable();

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        for (const auto& descriptor : request->stores_to_remove()) {
            auto storeId = FromProto<TStoreId>(descriptor.store_id());
            auto type = TypeFromId(storeId);

            if (IsChunkTabletStoreType(type)) {
                auto* chunk = chunkManager->GetChunkOrThrow(storeId);
                ValidateTabletContainsStore(tablet, chunk);
            } else if (type == EObjectType::ChunkView) {
                auto* chunkView = chunkManager->GetChunkViewOrThrow(storeId);
                ValidateTabletContainsStore(tablet, chunkView);
            } else if (IsDynamicTabletStoreType(type)) {
                if (table->GetMountedWithEnabledDynamicStoreRead()) {
                    auto* dynamicStore = chunkManager->GetDynamicStoreOrThrow(storeId);
                    ValidateTabletContainsStore(tablet, dynamicStore);
                }
            } else {
                THROW_ERROR_EXCEPTION("Cannot detach store %v of type %v from tablet %v",
                    storeId,
                    type,
                    tablet->GetId());
            }
        }
    }

    void PrepareUpdateTabletStoresOrdered(
        TTablet* tablet,
        NProto::TReqUpdateTabletStores* request)
    {
        auto tabletId = tablet->GetId();
        const auto* table = tablet->GetTable();
        const auto* tabletChunkList = tablet->GetChunkList();

        if (request->stores_to_add_size() > 0) {
            if (request->stores_to_add_size() > 1) {
                THROW_ERROR_EXCEPTION("Cannot attach more than one store to an ordered tablet %v at once",
                    tabletId);
            }

            const auto& descriptor = request->stores_to_add(0);
            auto storeId = FromProto<TStoreId>(descriptor.store_id());
            YT_VERIFY(descriptor.has_starting_row_index());
            if (tabletChunkList->Statistics().LogicalRowCount != descriptor.starting_row_index()) {
                THROW_ERROR_EXCEPTION("Invalid starting row index of store %v in tablet %v: expected %v, got %v",
                    storeId,
                    tabletId,
                    tabletChunkList->Statistics().LogicalRowCount,
                    descriptor.starting_row_index());
            }
        }

        auto updateReason = FromProto<ETabletStoresUpdateReason>(request->update_reason());

        if (updateReason == ETabletStoresUpdateReason::Trim) {
            int childIndex = tabletChunkList->GetTrimmedChildCount();
            const auto& children = tabletChunkList->Children();
            for (const auto& descriptor : request->stores_to_remove()) {
                auto storeId = FromProto<TStoreId>(descriptor.store_id());
                if (TypeFromId(storeId) == EObjectType::OrderedDynamicTabletStore) {
                    continue;
                }

                if (childIndex >= std::ssize(children)) {
                    THROW_ERROR_EXCEPTION("Attempted to trim store %v which is not part of tablet %v",
                        storeId,
                        tabletId);
                }
                if (children[childIndex]->GetId() != storeId) {
                    THROW_ERROR_EXCEPTION("Invalid store to trim in tablet %v: expected %v, got %v",
                        tabletId,
                        children[childIndex]->GetId(),
                        storeId);
                }
                ++childIndex;
            }
        }

        if (updateReason == ETabletStoresUpdateReason::Flush &&
            IsDynamicStoreReadEnabled(table, GetDynamicConfig()) &&
            !request->stores_to_remove().empty())
        {
            auto storeId = FromProto<TStoreId>(request->stores_to_remove(0).store_id());
            int firstDynamicStoreIndex = GetFirstDynamicStoreIndex(tabletChunkList);
            const auto* firstDynamicStore = tabletChunkList->Children()[firstDynamicStoreIndex];
            if (firstDynamicStore->GetId() != storeId) {
                THROW_ERROR_EXCEPTION("Attempted to flush ordered dynamic store out of order")
                    << TErrorAttribute("first_dynamic_store_id", firstDynamicStore->GetId())
                    << TErrorAttribute("flushed_store_id", storeId);
            }
        }
    }

    void AttachChunksToTablet(TTabletBase* tablet, const std::vector<TChunkTree*>& chunkTrees)
    {
        auto* chunkList = tablet->GetChunkList();
        auto* hunkChunkList = tablet->GetHunkChunkList();

        std::vector<TChunkTree*> storeChildren;
        std::vector<TChunkTree*> hunkChildren;
        storeChildren.reserve(chunkTrees.size());
        hunkChildren.reserve(chunkTrees.size());
        for (auto* child : chunkTrees) {
            if (IsHunkChunk(tablet, child)) {
                // NB: It is OK to try to attach hunk chunk multiple times.
                // Tablet node will take care of reference tracking and will detach
                // it only when it is not required by any store.
                if (hunkChunkList->HasChild(child)) {
                    continue;
                }
                hunkChildren.push_back(child);
            } else {
                storeChildren.push_back(child);
            }
        }

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        chunkManager->AttachToChunkList(chunkList, storeChildren);
        chunkManager->AttachToChunkList(hunkChunkList, hunkChildren);
    }
};

////////////////////////////////////////////////////////////////////////////////

ITabletChunkManagerPtr CreateTabletChunkManager(TBootstrap* bootstrap)
{
    return New<TTabletChunkManager>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
