#include "coordinate_controller.h"
#include "evaluate_controller.h"

#include "private.h"
#include "helpers.h"

#include "ast.h"
#include "ast_visitor.h"
#include "ast_helpers.h"

#include "graphviz.h"

#include <ytlib/new_table_client/reader.h>
#include <ytlib/new_table_client/chunk_writer.h>

#include <core/misc/protobuf_helpers.h>

#include <core/concurrency/fiber.h>

namespace NYT {
namespace NQueryClient {

using namespace NConcurrency;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

TCoordinateController::TCoordinateController(
    ICoordinateCallbacks* callbacks,
    const TQueryFragment& fragment,
    TWriterPtr writer)
    : Callbacks_(callbacks)
    , Fragment_(fragment)
    , Writer_(std::move(writer))
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
        case EObjectType::QueryFragment:
            return GetPeer(CounterFromId(objectId));
        default:
            return GetCallbacks()->GetReader(dataSplit);
    }
}

TError TCoordinateController::Run()
{
    ViewFragment(Fragment_, "Coordinator -> Before");

    SplitFurther();
    PushdownFilters();
    PushdownProjects();

    ViewFragment(Fragment_, "Coordinator -> After");

    DelegateToPeers();

    ViewFragment(Fragment_, "Coordinator -> Final");

    return New<TEvaluateController>(this, Fragment_, Writer_)->Run();
}

IReaderPtr TCoordinateController::GetPeer(int i)
{
    YCHECK(i < Peers_.size());
    return Peers_[i];
}

void TCoordinateController::SplitFurther()
{
    LOG_DEBUG("Splitting inputs");
    // Rewrite
    //   S
    // to
    //   U -> { S1 ... Sk }
    Rewrite(
    [this] (TQueryContext* context, const TOperator* op) -> const TOperator* {
        if (auto* scanOp = op->As<TScanOperator>()) {
            auto objectId = GetObjectIdFromDataSplit(scanOp->DataSplit());
            LOG_DEBUG("Splitting input %s", ~ToString(objectId));

            auto dataSplitsOrError = WaitFor(
                GetCallbacks()->SplitFurther(scanOp->DataSplit()));
            auto dataSplits = dataSplitsOrError.GetValueOrThrow();
            LOG_DEBUG(
                "Got %" PRISZT " splits for input %s",
                dataSplits.size(),
                ~ToString(objectId));

            if (dataSplits.size() == 1) {
                const auto& dataSplit = dataSplits[0];
                auto* splittedScanOp = new (context) TScanOperator(
                    context,
                    scanOp->GetTableIndex());
                splittedScanOp->DataSplit() = dataSplit;
                return splittedScanOp;
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
    [] (TQueryContext* context, const TOperator* op) -> const TOperator* {
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
    [this] (TQueryContext* context, const TOperator* op) -> const TOperator* {
        // Rewrute
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

void TCoordinateController::DelegateToPeers()
{
    LOG_DEBUG("Delegating subfragments to peers");
    YCHECK(Peers_.empty());

    int numberOfScanOperators = 0;
    Visit(GetHead(), [&] (const TOperator* op) {
        if (op->IsA<TScanOperator>()) {
            ++numberOfScanOperators;
        }
    });

    LOG_DEBUG("Got %d scan operators in fragment", numberOfScanOperators);
    if (numberOfScanOperators <= 1) {
        LOG_DEBUG("Nothing to delegate");
        return;
    }

    // TODO(sandello): In general case, there could be some final aggregation.
    YCHECK(GetHead()->IsA<TUnionOperator>());
    auto* unionOp = GetHead()->As<TUnionOperator>();
    auto* facadeUnionOp = new (GetContext()) TUnionOperator(GetContext());

    for (const auto& source : unionOp->Sources()) {
        auto fragment = TQueryFragment(GetContext(), source);
        auto peer = GetCallbacks()->Delegate(fragment, GetHeaviestSplit(source));

        Peers_.emplace_back(std::move(peer));

        LOG_DEBUG("Delegated subfragment %s", ~ToString(fragment.Guid()));

        auto* facadeScanOp = new (GetContext()) TScanOperator(
            GetContext(),
            GetContext()->GetFakeTableIndex());

        SetObjectId(
            &facadeScanOp->DataSplit(),
            MakeId(EObjectType::QueryFragment, 0xBABE, Peers_.size() - 1, 0));
        SetTableSchema(
            &facadeScanOp->DataSplit(),
            InferTableSchema(source));
        SetKeyColumns(
            &facadeScanOp->DataSplit(),
            InferKeyColumns(source));

        facadeUnionOp->Sources().push_back(facadeScanOp);
    }

    SetHead(facadeUnionOp);

    LOG_DEBUG("Delegated %" PRISZT " subfragments to peers", Peers_.size());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

