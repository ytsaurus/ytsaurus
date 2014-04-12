#include "stdafx.h"
#include "coordinator.h"

#include "private.h"
#include "helpers.h"

#include "plan_node.h"
#include "plan_visitor.h"
#include "plan_helpers.h"

#include "graphviz.h"

#include <core/concurrency/scheduler.h>

#include <core/misc/protobuf_helpers.h>

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

TCoordinator::TCoordinator(
    ICoordinateCallbacks* callbacks,
    const TPlanFragment& fragment)
    : Callbacks_(callbacks)
    , Fragment_(fragment)
    , Logger(QueryClientLogger)
{
    Logger.AddTag(Sprintf(
        "FragmendId: %s",
        ~ToString(Fragment_.Id())));
}

TCoordinator::~TCoordinator()
{ }

TError TCoordinator::Run()
{
    try {
        LOG_DEBUG("Coordinating plan fragment");

        // Infer key range and push it down.
        auto keyRange = Fragment_.GetHead()->GetKeyRange();
        auto keyRangeFormatter = [] (const TKeyRange& range) -> Stroka {
            return Sprintf("[%s .. %s]",
                ~ToString(range.first),
                ~ToString(range.second));
        };
        Fragment_.Rewrite([&] (TPlanContext* context, const TOperator* op) -> const TOperator* {
            if (auto* scanOp = op->As<TScanOperator>()) {
                auto* clonedScanOp = scanOp->Clone(context)->As<TScanOperator>();
                for (auto& split : clonedScanOp->DataSplits()) {
                    auto originalRange = GetBothBoundsFromDataSplit(split);
                    auto intersectedRange = Intersect(originalRange, keyRange);
                    LOG_DEBUG("Narrowing split %s key range from %s to %s",
                        ~ToString(GetObjectIdFromDataSplit(split)),
                        ~keyRangeFormatter(originalRange),
                        ~keyRangeFormatter(intersectedRange));
                    SetBothBounds(&split, intersectedRange);
                }
                return clonedScanOp;
            }
            return op;
        });

        // Now build and distribute fragments.
        Fragment_ = TPlanFragment(
            Fragment_.GetContext(),
            Simplify(Gather(Scatter(Fragment_.GetHead()))));

        DelegateToPeers();

        return TError();
    } catch (const std::exception& ex) {
        auto error = TError("Failed to coordinate query fragment") << ex;
        LOG_ERROR(error);
        return error;
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
        result.emplace_back(std::get<0>(peer));
    }
    return result;
}

std::pair<std::vector<const TOperator*>, TSchema> TCoordinator::Scatter(const TOperator* op)
{
    auto* context = Fragment_.GetContext().Get();
    std::vector<const TOperator*> resultOps;

    TSchema schema = std::make_pair(op->GetTableSchema(), op->GetKeyColumns());

    switch (op->GetKind()) {

        case EOperatorKind::Scan: {
            auto* scanOp = op->As<TScanOperator>();
            auto groupedSplits = Regroup(Split(scanOp->DataSplits()));

            for (const auto& splits : groupedSplits) {
                auto* newScanOp = scanOp->Clone(context)->As<TScanOperator>();
                newScanOp->DataSplits() = splits;
                resultOps.push_back(newScanOp);
            }

            break;
        }

        case EOperatorKind::Filter: {
            auto* filterOp = op->As<TFilterOperator>();

            resultOps = Scatter(filterOp->GetSource()).first;
            for (auto& resultOp : resultOps) {
                auto* newFilterOp = filterOp->Clone(context)->As<TFilterOperator>();
                newFilterOp->SetSource(resultOp);
                resultOp = newFilterOp;
            }

            break;
        }

        case EOperatorKind::Group: {
            auto* groupOp = op->As<TGroupOperator>();

            resultOps = Scatter(groupOp->GetSource()).first;
            for (auto& resultOp : resultOps) {
                auto* newGroupOp = groupOp->Clone(context)->As<TGroupOperator>();
                newGroupOp->SetSource(resultOp);
                resultOp = newGroupOp;
            }

            if (resultOps.size() <= 1) {
                break;
            }

            auto* finalGroupOp = context->TrackedNew<TGroupOperator>(Gather(std::make_pair(resultOps, schema)));

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

            resultOps = Scatter(projectOp->GetSource()).first;

            for (auto& resultOp : resultOps) {
                auto* newProjectOp = projectOp->Clone(context)->As<TProjectOperator>();
                newProjectOp->SetSource(resultOp);
                resultOp = newProjectOp;
            }

            break;
        }

    }

    return std::make_pair(resultOps, schema);
}

const TOperator* TCoordinator::Gather(const std::pair<std::vector<const TOperator*>, TSchema>& ops)
{
    std::function<const TDataSplit&(const TOperator*)> determineCollocatedSplit =
        [&determineCollocatedSplit] (const TOperator* op) -> const TDataSplit& {
            switch (op->GetKind()) {
                case EOperatorKind::Scan:
                    return op->As<TScanOperator>()->DataSplits().front();
                case EOperatorKind::Filter:
                    return determineCollocatedSplit(op->As<TFilterOperator>()->GetSource());
                case EOperatorKind::Group:
                    return determineCollocatedSplit(op->As<TGroupOperator>()->GetSource());
                case EOperatorKind::Project:
                    return determineCollocatedSplit(op->As<TProjectOperator>()->GetSource());
            }
            YUNREACHABLE();
        };

    auto* context = Fragment_.GetContext().Get();

    auto* resultOp = context->TrackedNew<TScanOperator>();
    auto& resultSplits = resultOp->DataSplits();

    resultOp->SetTableSchema(ops.second.first);
    resultOp->SetKeyColumns(ops.second.second);

    for (const auto& op : ops.first) {
        auto fragment = TPlanFragment(context, op);
        LOG_DEBUG("Created subfragment (SubFragmentId: %s)",
            ~ToString(fragment.Id()));

        int index = Peers_.size();
        Peers_.emplace_back(fragment, determineCollocatedSplit(op), nullptr);

        TDataSplit facadeSplit;

        SetObjectId(
            &facadeSplit,
            MakeId(EObjectType::PlanFragment, 0xbabe, index, 0xc0ffee));
        SetTableSchema(&facadeSplit, op->GetTableSchema());
        SetKeyColumns(&facadeSplit, op->GetKeyColumns());
        SetBothBounds(&facadeSplit, op->GetKeyRange());

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
            auto outerPair = IsInternal(outerSplit);
            if (!outerPair.first) {
                return op;
            }

            YCHECK(outerPair.second < Peers_.size());
            const auto& peer = Peers_[outerPair.second];

            const auto& innerSplit = std::get<1>(peer);
            auto innerPair = IsInternal(innerSplit);
            if (!innerPair.first) {
                return op;
            }

            return std::get<0>(peer).GetHead();
        });
}

TDataSplits TCoordinator::Split(const TDataSplits& splits)
{
    TDataSplits result;

    for (const auto& split : splits) {
        auto objectId = GetObjectIdFromDataSplit(split);

        if (Callbacks_->CanSplit(split)) {
            LOG_DEBUG("Splitting input %s", ~ToString(objectId));
        } else {
            result.push_back(split);
            continue;
        }

        auto newSplitsOrError = WaitFor(Callbacks_->SplitFurther(split, Fragment_.GetContext()));
        auto newSplits = newSplitsOrError.ValueOrThrow();

        LOG_DEBUG(
            "Got %" PRISZT " splits for input %s",
            newSplits.size(),
            ~ToString(objectId));

        result.insert(result.end(), newSplits.begin(), newSplits.end());
    }

    return result;
}

TGroupedDataSplits TCoordinator::Regroup(const TDataSplits& splits)
{
    return Callbacks_->Regroup(splits, Fragment_.GetContext());
}

std::pair<bool, int> TCoordinator::IsInternal(const TDataSplit& split)
{
    auto objectId = GetObjectIdFromDataSplit(split);
    auto type = TypeFromId(objectId);
    int counter = static_cast<int>(CounterFromId(objectId));

    if (type == EObjectType::PlanFragment) {
        return std::make_pair(true, counter);
    } else {
        return std::make_pair(false, -1);
    }
}

void TCoordinator::DelegateToPeers()
{
    for (auto& peer : Peers_) {
        if (!IsInternal(std::get<1>(peer)).first) {
            std::get<2>(peer) = Callbacks_->Delegate(std::get<0>(peer), std::get<1>(peer));
        }
    }
}

ISchemafulReaderPtr TCoordinator::GetReader(
    const TDataSplit& split,
    TPlanContextPtr context)
{
    auto objectId = GetObjectIdFromDataSplit(split);
    LOG_DEBUG("Creating reader for %s", ~ToString(objectId));

    auto pair = IsInternal(split);
    if (pair.first) {
        YCHECK(pair.second < Peers_.size());
        return std::get<2>(Peers_[pair.second]);
    } else {
        return Callbacks_->GetReader(split, context);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

