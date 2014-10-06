#include "stdafx.h"
#include "coordinator.h"

#include "private.h"
#include "helpers.h"

#include "plan_helpers.h"

#include <core/concurrency/scheduler.h>

#include <core/profiling/scoped_timer.h>

#include <core/misc/protobuf_helpers.h>

#include <core/tracing/trace_context.h>

#include <ytlib/chunk_client/chunk_replica.h>

#include <ytlib/new_table_client/schemaful_reader.h>
#include <ytlib/new_table_client/writer.h>
#include <ytlib/new_table_client/schema.h>
#include <ytlib/new_table_client/unversioned_row.h>

#include <ytlib/object_client/helpers.h>

namespace NYT {
namespace NQueryClient {

using namespace NConcurrency;
using namespace NObjectClient;
using namespace NVersionedTableClient;

////////////////////////////////////////////////////////////////////////////////

static const int MaxCounter = std::numeric_limits<int>::max();

////////////////////////////////////////////////////////////////////////////////

TCoordinator::TCoordinator(
    ICoordinateCallbacks* callbacks,
    const TPlanFragmentPtr& fragment)
    : Callbacks_(callbacks)
    , Fragment_(fragment)
    , Logger(QueryClientLogger)
{
    Logger.AddTag("FragmentId: %v", Fragment_->GetId());
}

TCoordinator::~TCoordinator()
{ }

void TCoordinator::Run()
{
    TRACE_CHILD("QueryClient", "Coordinate") {
        TRACE_ANNOTATION("fragment_id", Fragment_->GetId());
        QueryStat = TQueryStatistics();
        TDuration wallTime;

        try {
            LOG_DEBUG("Coordinating plan fragment");
            NProfiling::TAggregatingTimingGuard timingGuard(&wallTime);

            // Now build and distribute fragments.                
            Fragment_ = Fragment_->RewriteWith(Gather(
                Scatter(Fragment_->Head),
                Fragment_->Head->GetTableSchema(),
                Fragment_->Head->GetKeyColumns()));

            DelegateToPeers();

            QueryStat.SyncTime = wallTime - QueryStat.AsyncTime;
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Failed to coordinate query fragment") << ex;
        }
    }
}

TPlanFragmentPtr TCoordinator::GetCoordinatorFragment() const
{
    return Fragment_;
}

std::vector<TPlanFragmentPtr> TCoordinator::GetPeerFragments() const
{
    std::vector<TPlanFragmentPtr> result;
    result.reserve(Peers_.size());
    for (const auto& peer : Peers_) {
        result.emplace_back(peer.Fragment);
    }
    return result;
}

TQueryStatistics TCoordinator::GetStatistics() const
{
    TQueryStatistics result;

    for (const auto& peer : Peers_) {
        TQueryStatistics subResult = peer.QueryResult.Get().Value();

        result.RowsRead += subResult.RowsRead;
        result.RowsWritten += subResult.RowsWritten;
        result.SyncTime += subResult.SyncTime;
        result.AsyncTime += subResult.AsyncTime;
        result.IncompleteInput |= subResult.IncompleteInput;
        result.IncompleteOutput |= subResult.IncompleteOutput;
    }

    result.SyncTime += QueryStat.SyncTime;
    result.AsyncTime += QueryStat.AsyncTime;    

    return result;
}

std::vector<TOperatorPtr> TCoordinator::Scatter(
    const TConstOperatorPtr& op,
    const TKeyTrieNode& keyTrie /*= TKeyTrieNode()*/)
{
    std::vector<TOperatorPtr> resultOps;

    if (auto scanOp = op->As<TScanOperator>()) {
        auto groupedSplits = SplitAndRegroup(
            scanOp->DataSplits,
            keyTrie);

        for (const auto& splits : groupedSplits) {
            auto newScanOp = New<TScanOperator>();
            newScanOp->DataSplits = splits;
            newScanOp->TableSchema = scanOp->GetTableSchema();
            newScanOp->KeyColumns = scanOp->GetKeyColumns();
            resultOps.push_back(std::move(newScanOp));
        }
    } else if (auto filterOp = op->As<TFilterOperator>()) {
        TRowBuffer rowBuffer;
        auto predicateConstraints = ExtractMultipleConstraints(
            filterOp->Predicate,
            filterOp->Source->GetKeyColumns(),
            &rowBuffer);
        auto resultConstraints = IntersectKeyTrie(predicateConstraints, keyTrie);            

        resultOps = Scatter(filterOp->Source, resultConstraints);
        for (auto& resultOp : resultOps) {
            auto newFilterOp = New<TFilterOperator>();
            newFilterOp->Source = resultOp;

            auto predicate = filterOp->Predicate;

            if (auto scanOp = resultOp->As<TScanOperator>()) {
                if (scanOp->DataSplits.size() > 0) {
                    TKeyRange keyRange = GetBothBoundsFromDataSplit(scanOp->DataSplits[0]);
                    auto keyColumns = GetKeyColumnsFromDataSplit(scanOp->DataSplits[0]);
                    
                    for (size_t i = 1; i < scanOp->DataSplits.size(); ++i) {
                        keyRange = Unite(keyRange, GetBothBoundsFromDataSplit(scanOp->DataSplits[i]));
                    }

                    int rangeSize = std::min(keyRange.first.GetCount(), keyRange.second.GetCount());

                    size_t commonPrefixSize = 0;
                    while (commonPrefixSize < rangeSize && keyRange.first[commonPrefixSize] == keyRange.second[commonPrefixSize]) {
                        commonPrefixSize++;
                    }

                    if (commonPrefixSize < rangeSize) {
                        commonPrefixSize++;
                    }

                    predicate = RefinePredicate(keyRange, commonPrefixSize, predicate, keyColumns);
                }
            }

            newFilterOp->Predicate = predicate;
            resultOp = std::move(newFilterOp);
        }
    } else if (auto groupOp = op->As<TGroupOperator>()) {
        resultOps = Scatter(groupOp->Source);
        for (auto& resultOp : resultOps) {
            auto newGroupOp = New<TGroupOperator>();
            newGroupOp->Source = resultOp;
            newGroupOp->GroupItems = groupOp->GroupItems;
            newGroupOp->AggregateItems = groupOp->AggregateItems;
            resultOp = std::move(newGroupOp);
        }

        if (resultOps.size() > 1) {
            auto finalGroupOp = New<TGroupOperator>();
            finalGroupOp->Source = Gather(
                resultOps,
                resultOps.front()->GetTableSchema(),
                resultOps.front()->GetKeyColumns());

            auto& finalGroupItems = finalGroupOp->GroupItems;
            for (const auto& groupItem : groupOp->GroupItems) {
                auto referenceExpr = New<TReferenceExpression>(
                    NullSourceLocation,
                    groupItem.Expression->Type,
                    groupItem.Name);
                finalGroupItems.emplace_back(std::move(referenceExpr), groupItem.Name);
            }

            auto& finalAggregateItems = finalGroupOp->AggregateItems;
            for (const auto& aggregateItem : groupOp->AggregateItems) {
                auto referenceExpr = New<TReferenceExpression>(
                    NullSourceLocation,
                    aggregateItem.Expression->Type,
                    aggregateItem.Name);
                finalAggregateItems.emplace_back(
                    std::move(referenceExpr),
                    aggregateItem.AggregateFunction,
                    aggregateItem.Name);
            }

            resultOps.clear();
            resultOps.push_back(finalGroupOp);
        }
    } else if (auto projectOp = op->As<TProjectOperator>()) {
        resultOps = Scatter(projectOp->Source, keyTrie);

        for (auto& resultOp : resultOps) {
            auto newProjectOp = New<TProjectOperator>();
            newProjectOp->Source = resultOp;
            newProjectOp->Projections = projectOp->Projections;
            resultOp = std::move(newProjectOp);
        }
    } else {
        YUNREACHABLE();
    }

    return resultOps;
}

TOperatorPtr TCoordinator::Gather(
    const std::vector<TOperatorPtr>& ops,
    const TTableSchema& tableSchema,
    const TKeyColumns& keyColumns)
{
    std::function<const TDataSplit&(const TConstOperatorPtr&)> getCollocatedSplit =
        [&getCollocatedSplit] (const TConstOperatorPtr& op) -> const TDataSplit& {
            if (auto scanOp = op->As<TScanOperator>()) {
                return scanOp->DataSplits[0];
            } else if (auto filterOp = op->As<TFilterOperator>()) {
                return getCollocatedSplit(filterOp->Source);
            } else if (auto groupOp = op->As<TGroupOperator>()) {
                return getCollocatedSplit(groupOp->Source);
            } else if (auto projectOp = op->As<TProjectOperator>()) {
                return getCollocatedSplit(projectOp->Source);
            } else {
                YUNREACHABLE();
            }
        };

    if (ops.size() == 1 && GetPlanFragmentPeer(getCollocatedSplit(ops.front()))) {
        return ops.front();
    } else {
        auto resultOp = New<TScanOperator>();
        resultOp->TableSchema = tableSchema;
        resultOp->KeyColumns = keyColumns;
        auto& resultSplits = resultOp->DataSplits;
    
        for (const auto& op : ops) {
            auto& collocatedSplit = getCollocatedSplit(op);

            YCHECK(!GetPlanFragmentPeer(collocatedSplit));

            auto fragment = Fragment_->RewriteWith(op);

            LOG_DEBUG("Created subfragment (SubfragmentId: %v)",
                fragment->GetId());

            int index = Peers_.size();
            Peers_.emplace_back(fragment, collocatedSplit, nullptr, Null);

            TDataSplit facadeSplit;

            SetObjectId(
                &facadeSplit,
                MakeId(EObjectType::PlanFragment, 0xbabe, index, 0xc0ffee));
            SetTableSchema(&facadeSplit, op->GetTableSchema());
            SetKeyColumns(&facadeSplit, op->GetKeyColumns());

            resultSplits.push_back(facadeSplit);
        }

        return resultOp;
    }
}

TGroupedDataSplits TCoordinator::SplitAndRegroup(
    const TDataSplits& splits,
    const TKeyTrieNode& keyTrie)
{
    TGroupedDataSplits result;
    TDataSplits allSplits;

    for (const auto& split : splits) {
        auto objectId = GetObjectIdFromDataSplit(split);

        if (Callbacks_->CanSplit(split)) {
            LOG_DEBUG("Splitting input %v", objectId);
        } else {
            allSplits.push_back(split);
            continue;
        }

        TDataSplits newSplits;

        {
            NProfiling::TAggregatingTimingGuard timingGuard(&QueryStat.AsyncTime);
            auto newSplitsOrError = WaitFor(Callbacks_->SplitFurther(split, Fragment_->NodeDirectory));
            newSplits = newSplitsOrError.ValueOrThrow();
        }

        LOG_DEBUG(
            "Got %v splits for input %v",
            newSplits.size(),
            objectId);

        allSplits.insert(allSplits.end(), newSplits.begin(), newSplits.end());
    }

    auto keyRangeFormatter = [] (const TKeyRange& range) -> Stroka {
        return Format("[%v .. %v]",
            range.first,
            range.second);
    };

    LOG_DEBUG("Splitting %v splits according to ranges", allSplits.size());

    TDataSplits resultSplits;
    for (const auto& split : allSplits) {
        auto originalRange = GetBothBoundsFromDataSplit(split);

        std::vector<TKeyRange> ranges = 
            GetRangesFromTrieWithinRange(originalRange, keyTrie);

        for (const auto& range : ranges) {
            auto splitCopy = split;

            LOG_DEBUG("Narrowing split %v key range from %v to %v",
                    GetObjectIdFromDataSplit(splitCopy),
                    keyRangeFormatter(originalRange),
                    keyRangeFormatter(range));
            SetBothBounds(&splitCopy, range);

            resultSplits.push_back(std::move(splitCopy));
        }
    }

    LOG_DEBUG("Regrouping %v splits", resultSplits.size());

    result = Callbacks_->Regroup(resultSplits, Fragment_->NodeDirectory);

    return result;
}

TNullable<int> TCoordinator::GetPlanFragmentPeer(const TDataSplit& split)
{
    auto objectId = GetObjectIdFromDataSplit(split);
    auto type = TypeFromId(objectId);
    auto counter = static_cast<int>(CounterFromId(objectId));

    if (type == EObjectType::PlanFragment) {
        return TNullable<int>(counter);
    } else {
        return TNullable<int>();
    }
}

void TCoordinator::DelegateToPeers()
{
    for (auto& peer : Peers_) {
        YCHECK(!GetPlanFragmentPeer(peer.CollocatedSplit));

        LOG_DEBUG("Delegating subfragment (SubfragmentId: %v)",
            peer.Fragment->GetId());
        std::tie(peer.Reader, peer.QueryResult) = Callbacks_->Delegate(
            peer.Fragment,
            peer.CollocatedSplit);
    }
}

ISchemafulReaderPtr TCoordinator::GetReader(
    const TDataSplit& split,
    TNodeDirectoryPtr nodeDirectory)
{
    auto objectId = GetObjectIdFromDataSplit(split);
    LOG_DEBUG("Creating reader for %v", objectId);

    auto peerIndex = GetPlanFragmentPeer(split);

    if (peerIndex) {
        YCHECK(peerIndex.Get() < Peers_.size());
        return Peers_[peerIndex.Get()].Reader;
    }

    return Callbacks_->GetReader(split, nodeDirectory);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

