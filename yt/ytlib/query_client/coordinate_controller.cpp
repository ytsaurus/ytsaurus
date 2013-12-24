#include "coordinate_controller.h"
#include "evaluate_controller.h"

#include "private.h"
#include "helpers.h"

#include "plan_node.h"
#include "plan_visitor.h"
#include "plan_helpers.h"

#include "graphviz.h"

#include <ytlib/new_table_client/reader.h>
#include <ytlib/new_table_client/writer.h>
#include <ytlib/new_table_client/schema.h>

#include <ytlib/object_client/helpers.h>

#include <core/concurrency/fiber.h>

#include <core/misc/protobuf_helpers.h>

namespace NYT {
namespace NQueryClient {

using namespace NConcurrency;
using namespace NObjectClient;

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
        ~ToString(Fragment_.Guid())));
}

TCoordinateController::~TCoordinateController()
{ }

IReaderPtr TCoordinateController::GetReader(const TDataSplit& dataSplit)
{
    auto objectId = GetObjectIdFromDataSplit(dataSplit);
    LOG_DEBUG("Creating reader for %s", ~ToString(objectId));
    switch (TypeFromId(objectId)) {
        case EObjectType::QueryPlan: {
            auto peerIndex = CounterFromId(objectId);
            YASSERT(peerIndex < Peers_.size());
            auto peer = std::get<1>(Peers_[peerIndex]);
            YASSERT(peer);
            return peer;
        }
        default:
            return GetCallbacks()->GetReader(dataSplit);
    }
}

TError TCoordinateController::Run()
{
    try {
        LOG_DEBUG("Coordinating plan fragment");

        SplitFurther();
        PushdownFilters();
        PushdownGroupBys();
        PushdownProjects();
        DistributeToPeers();

        for (auto& peer : Peers_) {
            std::get<1>(peer) = GetCallbacks()->Delegate(
                std::get<0>(peer),
                GetHeaviestSplit(std::get<0>(peer).GetHead()));
        } 
    } catch (const std::exception& ex) {
        auto error = TError("Failed to coordinate plan fragment") << ex;
        LOG_ERROR(error);
        return error;
    }
    return TError();
}

std::vector<TPlanFragment> TCoordinateController::GetPeerSplits() const
{
    std::vector<TPlanFragment> result;
    result.reserve(Peers_.size());
    for (const auto& peer : Peers_) {
        result.emplace_back(std::get<0>(peer));
    }
    return result;
}

void TCoordinateController::SplitFurther()
{
    LOG_DEBUG("Splitting inputs");
    // Rewrite
    //   S
    // to
    //   U -> { S1 ... Sk }
    Rewrite(
    [this] (TPlanContext* context, const TOperator* op) -> const TOperator* {
        if (auto* scanOp = op->As<TScanOperator>()) {
            auto objectId = GetObjectIdFromDataSplit(scanOp->DataSplit());
            if (!GetCallbacks()->CanSplit(scanOp->DataSplit())) {
                LOG_DEBUG("Skipping input %s", ~ToString(objectId));
                return scanOp;
            } else {
                LOG_DEBUG("Splitting input %s", ~ToString(objectId));
            }

            auto dataSplitsOrError = WaitFor(
                GetCallbacks()->SplitFurther(scanOp->DataSplit()));
            auto dataSplits = dataSplitsOrError.GetValueOrThrow();
            LOG_DEBUG(
                "Got %" PRISZT " splits for input %s",
                dataSplits.size(),
                ~ToString(objectId));

            if (dataSplits.size() == 0) {
                THROW_ERROR_EXCEPTION("Input %s is empty", ~ToString(objectId));
            } else {
                auto* unionOp = new (context) TUnionOperator(context);
                for (const auto& dataSplit : dataSplits) {
                    auto* splittedScanOp = new (context) TScanOperator(
                        context,
                        scanOp->GetTableIndex());
                    splittedScanOp->DataSplit() = dataSplit;
                    unionOp->Sources().push_back(splittedScanOp);
                }
                return unionOp;
            }
        }
        return op;
    });
}

void TCoordinateController::PushdownFilters()
{
    LOG_DEBUG("Pushing down filter operators");
    Rewrite(
    [] (TPlanContext* context, const TOperator* op) -> const TOperator* {
        // Rewrite
        //   F -> U -> { O1 ... Ok }
        // to
        //   U -> { F -> O1 ... F -> Ok }
        if (auto* filterOp = op->As<TFilterOperator>()) {
            if (auto* unionOp = filterOp->GetSource()->As<TUnionOperator>()) {
                auto* newUnionOp = new (context) TUnionOperator(context);
                newUnionOp->Sources().reserve(unionOp->Sources().size());
                for (const auto& source : unionOp->Sources()) {
                    auto clonedFilterOp = new (context) TFilterOperator(
                        context,
                        source);
                    clonedFilterOp->SetPredicate(filterOp->GetPredicate());
                    newUnionOp->Sources().push_back(clonedFilterOp);
                }
                return newUnionOp;
            }
        }
        return op;
    });
}

void TCoordinateController::PushdownProjects()
{
    LOG_DEBUG("Pushing down project operators");
    Rewrite(
    [this] (TPlanContext* context, const TOperator* op) -> const TOperator* {
        // Rewrite
        //   P -> U -> { O1 ... Ok }
        // to
        //   U -> { P -> O1 ... P -> Ok }
        if (auto* projectOp = op->As<TProjectOperator>()) {
            if (auto* unionOp = projectOp->GetSource()->As<TUnionOperator>()) {
                auto* newUnionOp = new (context) TUnionOperator(context);
                newUnionOp->Sources().reserve(unionOp->Sources().size());
                for (const auto& source : unionOp->Sources()) {
                    auto clonedProjectOp = new (context) TProjectOperator(
                        context,
                        source);
                    clonedProjectOp->Projections() = projectOp->Projections();
                    newUnionOp->Sources().push_back(clonedProjectOp);
                }
                return newUnionOp;
            }
        }
        return op;
    });
}

void TCoordinateController::PushdownGroupBys()
{
    LOG_DEBUG("Pushing down group by operators");
    Rewrite(
    [this] (TPlanContext* context, const TOperator* op) -> const TOperator* {
        // Rewrite
        //   G -> U -> { O1 ... Ok }
        // to
        //   G -> U -> { G -> O1 ... G -> Ok }
        if (auto* groupByOp = op->As<TGroupByOperator>()) {
            if (auto* unionOp = groupByOp->GetSource()->As<TUnionOperator>()) {
                auto* newUnionOp = new (context) TUnionOperator(context);
                newUnionOp->Sources().reserve(unionOp->Sources().size());
                for (const auto& source : unionOp->Sources()) {
                    auto clonedGroupByOp = new (context) TGroupByOperator(
                        context,
                        source);
                    clonedGroupByOp->GroupItems() = groupByOp->GroupItems();
                    clonedGroupByOp->AggregateItems() = groupByOp->AggregateItems();
                    newUnionOp->Sources().push_back(clonedGroupByOp);
                }

                auto* finalGroupByOp = new (context) TGroupByOperator(context, newUnionOp);

                auto& finalGroupItems = finalGroupByOp->GroupItems();
                for (const auto& groupItem : groupByOp->GroupItems()) {
                    auto referenceExpr = new (context) TReferenceExpression(
                        context, NullSourceLocation, 
                        context->GetTableIndexByAlias(""), 
                        groupItem.Name);
                    finalGroupItems.push_back(TNamedExpression(
                        referenceExpr,
                        groupItem.Name));
                }

                auto& finalAggregateItems = finalGroupByOp->AggregateItems();
                for (const auto& aggregateItem : groupByOp->AggregateItems()) {
                    auto referenceExpr = new (context) TReferenceExpression(
                        context, 
                        NullSourceLocation, 
                        context->GetTableIndexByAlias(""), 
                        aggregateItem.Name);
                    finalAggregateItems.push_back(TAggregateItem(
                        referenceExpr,
                        aggregateItem.AggregateFunction,
                        aggregateItem.Name));
                }

                return finalGroupByOp;
            }
        }
        return op;
    });
}

void TCoordinateController::DistributeToPeers()
{
    LOG_DEBUG("Distributing plan to peers");
    YCHECK(Peers_.empty());

    int numberOfScanOperators = 0;
    Visit(GetHead(), [&] (const TOperator* op) {
        if (op->IsA<TScanOperator>()) {
            ++numberOfScanOperators;
        }
    });

    LOG_DEBUG("Got %d scan operators in plan fragment", numberOfScanOperators);
    if (numberOfScanOperators <= 1) {
        LOG_DEBUG("Nothing to distribute");
        return;
    }

    Rewrite(
    [this] (TPlanContext* context, const TOperator* op) -> const TOperator* {
        // Rewrite
        //   U -> { O1 ... Ok }
        // to
        //   U -> { S1 ... Sk } && S1 -> O1, ..., Sk -> Ok
        if (auto* unionOp = op->As<TUnionOperator>()) {
            auto* facadeUnionOp = new (GetContext()) TUnionOperator(GetContext());

            for (const auto& source : unionOp->Sources()) {
                auto fragment = TPlanFragment(GetContext(), source);
                Peers_.emplace_back(fragment, nullptr);

                LOG_DEBUG("Created subfragment %s", ~ToString(fragment.Guid()));

                auto* facadeScanOp = new (GetContext()) TScanOperator(
                    GetContext(),
                    GetContext()->GetFakeTableIndex());

                SetObjectId(
                    &facadeScanOp->DataSplit(),
                    MakeId(EObjectType::QueryPlan, 0xBABE, Peers_.size() - 1, 0));
                SetTableSchema(
                    &facadeScanOp->DataSplit(),
                    source->GetTableSchema());
                SetKeyColumns(
                    &facadeScanOp->DataSplit(),
                    source->GetKeyColumns());

                facadeUnionOp->Sources().push_back(facadeScanOp);
            }

            return facadeUnionOp;
        }

        return op;
    });

    LOG_DEBUG("Distributed %" PRISZT " subfragments to peers", Peers_.size());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

