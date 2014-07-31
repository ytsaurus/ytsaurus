#include "stdafx.h"
#include "coordinator.h"

#include "private.h"
#include "helpers.h"

#include "plan_node.h"
#include "plan_helpers.h"
#include "plan_visitor.h"

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
    const TPlanFragment& fragment)
    : Callbacks_(callbacks)
    , Fragment_(fragment)
    , Logger(QueryClientLogger)
{
    Logger.AddTag("FragmentId: %v", Fragment_.Id());
}

TCoordinator::~TCoordinator()
{ }

void TCoordinator::Run()
{
    TRACE_CHILD("QueryClient", "Coordinate") {
        TRACE_ANNOTATION("fragment_id", Fragment_.Id());
        QueryStat = TQueryStatistics();
        TDuration wallTime;

        try {
            LOG_DEBUG("Coordinating plan fragment");
            NProfiling::TAggregatingTimingGuard timingGuard(&wallTime);

            // Now build and distribute fragments.
            Fragment_ = TPlanFragment(
                Fragment_.GetContext(),
                Simplify(Gather(Scatter(Fragment_.GetHead()))));

            DelegateToPeers();

            QueryStat.SyncTime = wallTime - QueryStat.AsyncTime;
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Failed to coordinate query fragment") << ex;
        }
    }
}

TPlanFragment TCoordinator::GetCoordinatorFragment() const
{
    return Fragment_;
}

std::vector<TPlanFragment> TCoordinator::GetPeerFragments() const
{
    std::vector<TPlanFragment> result;
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

std::vector<const TOperator*> TCoordinator::Scatter(const TOperator* op, const TKeyTrieNode& keyTrie)
{
    auto* context = Fragment_.GetContext().Get();
    std::vector<const TOperator*> resultOps;

    switch (op->GetKind()) {

        case EOperatorKind::Scan: {
            auto* scanOp = op->As<TScanOperator>();
            auto groupedSplits = SplitAndRegroup(
                scanOp->DataSplits(),
                scanOp->GetTableSchema(),
                scanOp->GetKeyColumns(),
                keyTrie);

            for (const auto& splits : groupedSplits) {
                auto* newScanOp = scanOp->Clone(context)->As<TScanOperator>();
                newScanOp->DataSplits() = splits;
                resultOps.push_back(newScanOp);
            }

            break;
        }

        case EOperatorKind::Filter: {
            auto* filterOp = op->As<TFilterOperator>();

            TRowBuffer rowBuffer;
            auto predicateConstraints = ExtractMultipleConstraints(
                filterOp->GetPredicate(),
                InferKeyColumns(filterOp->GetSource()), &rowBuffer);
            auto resultConstraints = IntersectKeyTrie(predicateConstraints, keyTrie, &rowBuffer);            

            resultOps = Scatter(filterOp->GetSource(), resultConstraints);
            for (auto& resultOp : resultOps) {
                auto* newFilterOp = filterOp->Clone(context)->As<TFilterOperator>();
                newFilterOp->SetSource(resultOp);
                resultOp = newFilterOp;
            }

            break;
        }

        case EOperatorKind::Group: {
            auto* groupOp = op->As<TGroupOperator>();

            resultOps = Scatter(groupOp->GetSource(), keyTrie);
            for (auto& resultOp : resultOps) {
                auto* newGroupOp = groupOp->Clone(context)->As<TGroupOperator>();
                newGroupOp->SetSource(resultOp);
                resultOp = newGroupOp;
            }

            if (resultOps.size() <= 1) {
                break;
            }

            auto* finalGroupOp = context->TrackedNew<TGroupOperator>(Gather(resultOps));

            auto& finalGroupItems = finalGroupOp->GroupItems();
            for (const auto& groupItem : groupOp->GroupItems()) {
                auto referenceExpr = context->TrackedNew<TReferenceExpression>(
                    NullSourceLocation,
                    groupItem.Name);
                finalGroupItems.push_back(TNamedExpression(
                    referenceExpr,
                    groupItem.Name));
            }

            auto& finalAggregateItems = finalGroupOp->AggregateItems();
            for (const auto& aggregateItem : groupOp->AggregateItems()) {
                auto referenceExpr = context->TrackedNew<TReferenceExpression>(
                    NullSourceLocation,
                    aggregateItem.Name);
                finalAggregateItems.push_back(TAggregateItem(
                    referenceExpr,
                    aggregateItem.AggregateFunction,
                    aggregateItem.Name));
            }

            resultOps.clear();
            resultOps.push_back(finalGroupOp);

            break;
        }

        case EOperatorKind::Project: {
            auto* projectOp = op->As<TProjectOperator>();

            resultOps = Scatter(projectOp->GetSource(), keyTrie);

            for (auto& resultOp : resultOps) {
                auto* newProjectOp = projectOp->Clone(context)->As<TProjectOperator>();
                newProjectOp->SetSource(resultOp);
                resultOp = newProjectOp;
            }

            break;
        }

    }

    return resultOps;
}

const TOperator* TCoordinator::Gather(const std::vector<const TOperator*>& ops)
{
    YASSERT(!ops.empty());

    auto* context = Fragment_.GetContext().Get();

    auto* resultOp = context->TrackedNew<TScanOperator>();
    auto& resultSplits = resultOp->DataSplits();

    std::function<const TDataSplit&(const TOperator*)> collocatedSplit =
        [&collocatedSplit] (const TOperator* op) -> const TDataSplit& {
            switch (op->GetKind()) {
                case EOperatorKind::Scan:
                    return op->As<TScanOperator>()->DataSplits().front();
                case EOperatorKind::Filter:
                    return collocatedSplit(op->As<TFilterOperator>()->GetSource());
                case EOperatorKind::Group:
                    return collocatedSplit(op->As<TGroupOperator>()->GetSource());
                case EOperatorKind::Project:
                    return collocatedSplit(op->As<TProjectOperator>()->GetSource());
            }
            YUNREACHABLE();
        };

    for (const auto& op : ops) {
        auto fragment = TPlanFragment(context, op);
        LOG_DEBUG("Created subfragment (SubfragmentId: %v)",
            fragment.Id());

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

const TOperator* TCoordinator::Simplify(const TOperator* op)
{
    // If we have delegated a segment locally, then we can omit extra data copy.
    // Basically, we would like to reduce
    //   (peers) -> (first local query) -> (second local query)
    // to
    //   (peers) -> (first + second local query)
    return Apply(
        Fragment_.GetContext().Get(),
        op,
        [this] (const TPlanContext* context, const TOperator* op) -> const TOperator* {
            auto* scanOp = op->As<TScanOperator>();
            if (!scanOp || scanOp->DataSplits().size() != 1) {
                return op;
            }

            const auto& outerSplit = scanOp->DataSplits().front();
            auto outerExplanation = Explain(outerSplit);
            if (!outerExplanation.IsInternal || outerExplanation.IsEmpty) {
                return op;
            }

            YCHECK(outerExplanation.PeerIndex < Peers_.size());
            const auto& peer = Peers_[outerExplanation.PeerIndex];

            const auto& innerSplit = peer.CollocatedSplit;
            auto innerExplanation = Explain(innerSplit);
            if (!innerExplanation.IsInternal) {
                return op;
            }

            LOG_DEBUG("Keeping subfragment local (SubfragmentId: %v)",
                peer.Fragment.Id());

            return peer.Fragment.GetHead();
        });
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
            auto newSplitsOrError = WaitFor(Callbacks_->SplitFurther(split, Fragment_.GetContext()));
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

        result = Callbacks_->Regroup(resultSplits, Fragment_.GetContext());
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
                peer.Fragment.Id());
            std::tie(peer.Reader, peer.QueryResult) = Callbacks_->Delegate(
                peer.Fragment,
                peer.CollocatedSplit);
        } else {
            peer.QueryResult = MakePromise<TErrorOr<TQueryStatistics>>(TQueryStatistics()).ToFuture();
        }
    }
}

ISchemafulReaderPtr TCoordinator::GetReader(
    const TDataSplit& split,
    TPlanContextPtr context)
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

    return Callbacks_->GetReader(split, context);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

