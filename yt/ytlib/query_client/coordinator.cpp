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

class TEmptySchemafulReader
    : public ISchemafulReader
{
    virtual TAsyncError Open(const TTableSchema& /*schema*/) override
    {
        return OKFuture;
    }

    virtual bool Read(std::vector<TUnversionedRow>* /*rows*/) override
    {
        return false;
    }

    virtual TAsyncError GetReadyEvent() override
    {
        return OKFuture;
    }
};

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
            Fragment_ = Fragment_->RewriteWith(Gather(Scatter(Fragment_->Head)));

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
            scanOp->GetTableSchema(),
            scanOp->GetKeyColumns(),
            keyTrie);

        for (const auto& splits : groupedSplits) {
            auto newScanOp = New<TScanOperator>();
            newScanOp->DataSplits = splits;
            resultOps.push_back(std::move(newScanOp));
        }
    } else if (auto filterOp = op->As<TFilterOperator>()) {
        TRowBuffer rowBuffer;
        auto predicateConstraints = ExtractMultipleConstraints(
            filterOp->Predicate,
            Fragment_->Literals,
            filterOp->Source->GetKeyColumns(),
            &rowBuffer);
        auto resultConstraints = IntersectKeyTrie(predicateConstraints, keyTrie, &rowBuffer);            

        resultOps = Scatter(filterOp->Source, resultConstraints);
        for (auto& resultOp : resultOps) {
            auto newFilterOp = New<TFilterOperator>();
            newFilterOp->Source = resultOp;
            newFilterOp->Predicate = filterOp->Predicate;
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
            finalGroupOp->Source = Gather(resultOps);

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
    }

    return resultOps;
}

TOperatorPtr TCoordinator::Gather(const std::vector<TOperatorPtr>& ops)
{
    YASSERT(!ops.empty());

    auto resultOp = New<TScanOperator>();
    auto& resultSplits = resultOp->DataSplits;

    std::function<const TDataSplit&(const TConstOperatorPtr&)> collocatedSplit =
        [&collocatedSplit] (const TConstOperatorPtr& op) -> const TDataSplit& {
            if (auto scanOp = op->As<TScanOperator>()) {
                return scanOp->DataSplits[0];
            } else if (auto filterOp = op->As<TFilterOperator>()) {
                return collocatedSplit(filterOp->Source);
            } else if (auto groupOp = op->As<TGroupOperator>()) {
                return collocatedSplit(groupOp->Source);
            } else if (auto projectOp = op->As<TProjectOperator>()) {
                return collocatedSplit(projectOp->Source);
            } else {
                YUNREACHABLE();
            }
        };

    if (ops.size() == 1) {
        auto innerExplanation = Explain(collocatedSplit(ops.front()));

        if (innerExplanation.IsInternal) {
            return ops.front();
        }
    }

    for (const auto& op : ops) {
        auto fragment = Fragment_->RewriteWith(op);

        LOG_DEBUG("Created subfragment (SubfragmentId: %v)",
            fragment->GetId());

        int index = Peers_.size();
        Peers_.emplace_back(fragment, collocatedSplit(op), nullptr, Null);

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

TGroupedDataSplits TCoordinator::SplitAndRegroup(
    const TDataSplits& splits,
    const TTableSchema& tableSchema,
    const TKeyColumns& keyColumns,
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

    if (allSplits.empty()) {
        LOG_DEBUG("Adding an empty split");

        allSplits.emplace_back();
        auto& split = allSplits.back();

        SetObjectId(
            &split,
            MakeId(EObjectType::EmptyPlanFragment, 0xdead, MaxCounter, 0xc0ffee));
        SetTableSchema(&split, tableSchema);
        SetKeyColumns(&split, keyColumns);

        result.emplace_back(allSplits);
    } else {

        auto keyRangeFormatter = [] (const TKeyRange& range) -> Stroka {
            return Format("[%v .. %v]",
                range.first,
                range.second);
        };

        LOG_DEBUG("Splitting %v splits according to ranges", allSplits.size());

        TDataSplits resultSplits;
        for (const auto& split : allSplits) {
            auto originalRange = GetBothBoundsFromDataSplit(split);

            auto keySize = GetKeyColumnsFromDataSplit(split).size();

            TRowBuffer rowBuffer;
            std::vector<TKeyRange> ranges = 
                GetRangesFromTrieWithinRange(originalRange, &rowBuffer, keySize, keyTrie);

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
    }

    return result;
}

TCoordinator::TDataSplitExplanation TCoordinator::Explain(const TDataSplit& split)
{
    TDataSplitExplanation explanation;

    auto objectId = GetObjectIdFromDataSplit(split);
    auto type = TypeFromId(objectId);
    auto counter = static_cast<int>(CounterFromId(objectId));

    explanation.IsInternal = true;
    explanation.IsEmpty = false;
    explanation.PeerIndex = MaxCounter;

    switch (type) {
        case EObjectType::PlanFragment:
            explanation.PeerIndex = counter;
            break;
        case EObjectType::EmptyPlanFragment:
            explanation.IsEmpty = true;
            break;
        default:
            explanation.IsInternal = false;
            break;
    }

    return explanation;
}

void TCoordinator::DelegateToPeers()
{
    for (auto& peer : Peers_) {
        auto explanation = Explain(peer.CollocatedSplit);
        if (!explanation.IsInternal) {
            LOG_DEBUG("Delegating subfragment (SubfragmentId: %v)",
                peer.Fragment->GetId());
            std::tie(peer.Reader, peer.QueryResult) = Callbacks_->Delegate(
                peer.Fragment,
                peer.CollocatedSplit);
        } else {
            peer.QueryResult = MakeFuture<TErrorOr<TQueryStatistics>>(TQueryStatistics());
        }
    }
}

ISchemafulReaderPtr TCoordinator::GetReader(
    const TDataSplit& split,
    TNodeDirectoryPtr nodeDirectory)
{
    auto objectId = GetObjectIdFromDataSplit(split);
    LOG_DEBUG("Creating reader for %v", objectId);

    auto explanation = Explain(split);

    if (explanation.IsEmpty) {
        return New<TEmptySchemafulReader>();
    }

    if (explanation.IsInternal) {
        YCHECK(explanation.PeerIndex < Peers_.size());
        return Peers_[explanation.PeerIndex].Reader;
    }

    return Callbacks_->GetReader(split, nodeDirectory);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

