#include "coordinator.h"

#include "query_fragment.h"
#include "query_context.h"

#include "ast.h"
#include "ast_visitor.h"

#include "private.h"

#include "stubs.h"

#include "graphviz.h"

#include <core/concurrency/fiber.h>

namespace NYT {
namespace NQueryClient {

using namespace NYT::NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = QueryClientLogger;

class TCoordinateController
{
public:
    TCoordinateController(ICoordinateCallbacks* callbacks, const TQueryFragment& fragment)
        : Callbacks_(callbacks)
        , Fragment_(fragment)
    { }

    IMegaReaderPtr Run();

    void SplitFurther();
    void PushdownFilters();
    void PushdownProjects();

    ICoordinateCallbacks* GetCallbacks()
    {
        return Callbacks_;
    }

    TQueryContext* GetContext()
    {
        return Fragment_.GetContext().Get();
    }

private:
    ICoordinateCallbacks* Callbacks_;
    TQueryFragment Fragment_;

    template <class TFunctor>
    inline void Rewrite(const TFunctor& functor)
    {
        Fragment_.SetHead(Apply(GetContext(), Fragment_.GetHead(), functor));
    }
};

IMegaReaderPtr TCoordinateController::Run()
{
    ViewFragment(Fragment_, "BeforeCoordinate");
    SplitFurther();
    ViewFragment(Fragment_, "AfterSplitFurther");
    PushdownFilters();
    PushdownProjects();
    ViewFragment(Fragment_, "AfterPushdown");
    return nullptr;
}

void TCoordinateController::SplitFurther()
{
    // Rewrite
    //   S
    // to
    //   U -> { S1 ... Sk }
    Rewrite(
    [this] (TQueryContext* context, const TOperator* op) -> const TOperator*
    {
        if (auto* scanOp = op->As<TScanOperator>()) {
            LOG_DEBUG(
                "Splitting input %s",
                ~GetObjectIdFromDataSplit(scanOp->DataSplit()));
            auto dataSplitsOrError = WaitFor(
                GetCallbacks()->SplitFurther(scanOp->DataSplit()));
            auto dataSplits = dataSplitsOrError.GetValueOrThrow();
            LOG_DEBUG(
                "Got %" PRISZT " splits for input %s",
                dataSplits.size(),
                ~GetObjectIdFromDataSplit(scanOp->DataSplit()));
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
    Rewrite(
    [] (TQueryContext* context, const TOperator* op) -> const TOperator*
    {
        // Rewrite
        //   F -> U -> { N1 ... Nk }
        // to
        //   U -> { F -> N1 ... F -> Nk }
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
    Rewrite(
    [this] (TQueryContext* context, const TOperator* op) -> const TOperator*
    {
        // Rewrute
        //   P -> U -> { N1 ... Nk }
        // to
        //   U -> { P -> N1 ... F -> Nk }
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

class TCoordinator
    : public IExecutor
{
public:
    TCoordinator(ICoordinateCallbacks* callbacks)
        : Callbacks_(callbacks)
    { }

    virtual IMegaReaderPtr Execute(const TQueryFragment& fragment) override
    {
        return TCoordinateController(Callbacks_, fragment).Run();
    }

    DEFINE_BYVAL_RO_PROPERTY(ICoordinateCallbacks*, Callbacks);

};

IExecutorPtr CreateCoordinator(ICoordinateCallbacks* callbacks)
{
    return New<TCoordinator>(callbacks);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

