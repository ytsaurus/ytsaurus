#include "coordinate_controller.h"

#include "private.h"
#include "helpers.h"

#include "plan_node.h"
#include "plan_visitor.h"
#include "plan_helpers.h"

#include "graphviz.h"

#include <core/concurrency/fiber.h>

#include <core/misc/protobuf_helpers.h>

#include <ytlib/chunk_client/chunk_replica.h>

#include <ytlib/new_table_client/schemed_reader.h>
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

TCoordinateController::TCoordinateController(
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

TCoordinateController::~TCoordinateController()
{ }

TError TCoordinateController::Run()
{
    try {
        LOG_DEBUG("Coordinating plan fragment");

        Fragment_ = SplitPlanFragment(Fragment_);
        InitializeReaders();

        return TError();
    } catch (const std::exception& ex) {
        auto error = TError("Failed to coordinate query fragment") << ex;
        LOG_ERROR(error);
        return error;
    }
}

TPlanFragment TCoordinateController::GetCoordinatorFragment() const
{
    return Fragment_;
}

TDataSplits TCoordinateController::GetUnitedDataSplit(
    TPlanContext* context,
    std::map<Stroka, const TOperator*> operatorsByLocation)
{
    TDataSplits facadeDataSplits;

    for (auto& colocatedOperator : operatorsByLocation) {
        Stroka location = colocatedOperator.first;
        const TOperator* op = colocatedOperator.second;

        auto fragment = TPlanFragment(context, op);
        LOG_DEBUG("Created subfragment (SubfragmentId: %s)",
            ~ToString(fragment.Id()));

        auto inferredKeyRange = op->GetKeyRange();
        if (IsEmpty(inferredKeyRange)) {
            LOG_DEBUG("Subfragment is empty (SubfragmentId: %s)",
                ~ToString(fragment.Id()));
            continue;
        } else {
            LOG_DEBUG("Inferred key range %s ... %s (SubfragmentId: %s)",
                ~ToString(inferredKeyRange.first),
                ~ToString(inferredKeyRange.second),
                ~ToString(fragment.Id()));
        }

        fragment.Rewrite(
        [&] (TPlanContext* context, const TOperator* op) -> const TOperator* {
            auto* scanOp = op->As<TScanOperator>();
            if (!scanOp) {
                return op;
            }

            TDataSplits filteredDataSplits;

            for (const auto& dataSplit : scanOp->DataSplits()) {
                if (!IsSorted(dataSplit)) {
                    filteredDataSplits.push_back(dataSplit);
                } else {
                    auto intersectedKeyRange = Intersect(
                        GetBothBoundsFromDataSplit(dataSplit),
                        inferredKeyRange);

                    if (!IsEmpty(intersectedKeyRange)) {
                        TDataSplit clonedDataSplit = dataSplit;
                        SetBothBounds(&clonedDataSplit, intersectedKeyRange);
                        filteredDataSplits.push_back(dataSplit);
                    }
                }
            }

            auto* clonedScanOp = scanOp->Clone(context)->As<TScanOperator>();
            clonedScanOp->DataSplits() = filteredDataSplits;

            return clonedScanOp;
        });

        int index = Peers_.size();
        Peers_.emplace_back(TPlanFragment(context, op), location, nullptr);

        TDataSplit facadeDataSplit;
        SetObjectId(
            &facadeDataSplit,
            MakeId(EObjectType::QueryPlan, 0xBABE, index, 0));
        SetTableSchema(&facadeDataSplit, op->GetTableSchema());
        SetKeyColumns(&facadeDataSplit, op->GetKeyColumns());
        SetBothBounds(&facadeDataSplit, inferredKeyRange);

        facadeDataSplits.push_back(facadeDataSplit);
    }

    return facadeDataSplits;
}

TDataSplits TCoordinateController::SplitFurther(
    TPlanContext* context,
    const TDataSplits& dataSplits)
{
    TDataSplits resultingDataSplits;

    for (const auto& dataSplit : dataSplits) {
        auto objectId = GetObjectIdFromDataSplit(dataSplit);

        if (Callbacks_->CanSplit(dataSplit)) {
            LOG_DEBUG("Splitting input %s", ~ToString(objectId));
        } else {
            resultingDataSplits.push_back(dataSplit);
            continue;
        }

        auto newDataSplitsOrError = WaitFor(Callbacks_->SplitFurther(dataSplit, context));
        auto newDataSplits = newDataSplitsOrError.GetValueOrThrow();

        LOG_DEBUG(
            "Got %" PRISZT " splits for input %s",
            newDataSplits.size(),
            ~ToString(objectId));

        resultingDataSplits.insert(
            resultingDataSplits.end(),
            newDataSplits.begin(),
            newDataSplits.end());
    }

    return resultingDataSplits;
}

std::map<Stroka, const TOperator*> TCoordinateController::SplitOperator(
    TPlanContext* context,
    const TOperator* op)
{
    if (auto* scanOp = op->As<TScanOperator>()) {
        TLocationToDataSplits dataSplitsByLocation =
            Callbacks_->GroupByLocation(
                SplitFurther(context, scanOp->DataSplits()),
                context);
        std::map<Stroka, const TOperator*> result;

        for (const auto& sourceSplit : dataSplitsByLocation) {
            auto* splittedScanOp = scanOp->Clone(context)->As<TScanOperator>();
            splittedScanOp->DataSplits() = sourceSplit.second;

            result[sourceSplit.first] = splittedScanOp;
        }

        return result;
    } else if (auto* filterOp = op->As<TFilterOperator>()) {
        auto splittedSources = SplitOperator(context, filterOp->GetSource());

        for (auto& splittedSource : splittedSources) {
            auto* newFilterOp = filterOp->Clone(context)->As<TFilterOperator>();
            newFilterOp->SetSource(splittedSource.second);
            splittedSource.second = newFilterOp;
        }

        return splittedSources;
    } else if (auto* projectOp = op->As<TProjectOperator>()) {
        auto splittedSources = SplitOperator(context, projectOp->GetSource());

        for (auto& splittedSource : splittedSources) {
            auto* newProjectOp = projectOp->Clone(context)->As<TProjectOperator>();
            newProjectOp->SetSource(splittedSource.second);
            splittedSource.second = newProjectOp;
        }

        return splittedSources;
    } else if (auto* groupOp = op->As<TGroupOperator>()) {
        auto splittedSources = SplitOperator(context, groupOp->GetSource());

        if (splittedSources.size() > 1) { // create final group by
            for (auto& splittedSource : splittedSources) {
                auto delegatedGroupOp = new (context) TGroupOperator(
                    context,
                    splittedSource.second);

                delegatedGroupOp->GroupItems() = groupOp->GroupItems();
                delegatedGroupOp->AggregateItems() = groupOp->AggregateItems();

                splittedSource.second = delegatedGroupOp;
            }

            auto* finalScanOp = new (context) TScanOperator(context);
            finalScanOp->DataSplits() = GetUnitedDataSplit(context, splittedSources);

            auto* finalGroupOp = new (context) TGroupOperator(
                context,
                finalScanOp);

            auto& finalGroupItems = finalGroupOp->GroupItems();
            for (const auto& groupItem : groupOp->GroupItems()) {
                auto referenceExpr = new (context) TReferenceExpression(
                    context,
                    NullSourceLocation,
                    groupItem.Name);
                finalGroupItems.push_back(TNamedExpression(
                    referenceExpr,
                    groupItem.Name));
            }

            auto& finalAggregateItems = finalGroupOp->AggregateItems();
            for (const auto& aggregateItem : groupOp->AggregateItems()) {
                auto referenceExpr = new (context) TReferenceExpression(
                    context,
                    NullSourceLocation,
                    aggregateItem.Name);
                finalAggregateItems.push_back(TAggregateItem(
                    referenceExpr,
                    aggregateItem.AggregateFunction,
                    aggregateItem.Name));
            }

            std::map<Stroka, const TOperator*> result;

            result["DRIVER"] = finalGroupOp;

            return result;
        } else {
            for (auto& splittedSource : splittedSources) {
                auto* newGroupOp = groupOp->Clone(context)->As<TGroupOperator>();
                newGroupOp->SetSource(splittedSource.second);
                splittedSource.second = newGroupOp;
            }

            return splittedSources;
        }
    }

    YUNREACHABLE();
}

void TCoordinateController::InitializeReaders()
{
    for (auto& peer : Peers_) {
        std::get<2>(peer) = Callbacks_->Delegate(std::get<0>(peer), std::get<1>(peer));
    }
}

std::vector<TPlanFragment> TCoordinateController::GetPeerFragments() const
{
    std::vector<TPlanFragment> result;
    result.reserve(Peers_.size());
    for (const auto& peer : Peers_) {
        result.emplace_back(std::get<0>(peer));
    }
    return result;
}

ISchemedReaderPtr TCoordinateController::GetReader(
    const TDataSplit& dataSplit,
    TPlanContextPtr context)
{
    auto objectId = GetObjectIdFromDataSplit(dataSplit);
    LOG_DEBUG("Creating reader for %s", ~ToString(objectId));
    switch (TypeFromId(objectId)) {
        case EObjectType::QueryPlan: {
            auto peerIndex = CounterFromId(objectId);
            YASSERT(peerIndex < Peers_.size());
            auto peer = std::get<2>(Peers_[peerIndex]);
            YASSERT(peer);
            return peer;
        }
        default:
            return Callbacks_->GetReader(dataSplit, context);
    }
}

TPlanFragment TCoordinateController::SplitPlanFragment(const TPlanFragment& planFragment)
{
    auto context = planFragment.GetContext().Get();
    auto splittedSources = SplitOperator(context, planFragment.GetHead());
    const TOperator* resultOp = nullptr;

    if (splittedSources.size() == 1 && splittedSources.begin()->first == "DRIVER") {
        resultOp = splittedSources.begin()->second;
    } else {
        auto* finalScanOp = new (context) TScanOperator(context);
        finalScanOp->DataSplits() = GetUnitedDataSplit(context, splittedSources);
        resultOp = finalScanOp;
    }

    return TPlanFragment(context, resultOp);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

