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

};

////////////////////////////////////////////////////////////////////////////////

namespace {

class TSplitFurther
    : public TAstVisitor
{
public:
    explicit TSplitFurther(TCoordinateController* controller)
        : Controller_(controller)
    { }

    virtual bool Visit(TScanOperator* op) override
    {
        // TODO(sandello): Parallellize this in case of multiple tables.
        auto dataSplitsOrError = WaitFor(Controller_->GetCallbacks()
            ->SplitFurther(op->DataSplit()));
        THROW_ERROR_EXCEPTION_IF_FAILED(dataSplitsOrError);
        const auto& dataSplits = dataSplitsOrError.GetValue();

        auto unionOp = 
            new (Controller_->GetContext())
            TUnionOperator(Controller_->GetContext());

        op->SwapWith(unionOp);

        for (const auto& dataSplit : dataSplits) {
            auto scanOp =
                new (Controller_->GetContext())
                TScanOperator(Controller_->GetContext(), op->GetTableIndex());
            scanOp->DataSplit() = dataSplit;
            unionOp->AddChild(scanOp);
        }

        return true;
    }

private:
    TCoordinateController* Controller_;

};

class TPushdown
    : public TAstVisitor
{
public:
    explicit TPushdown(TCoordinateController* controller, TOperator** head)
        : Controller_(controller)
        , Head_(head)
    { }

    template <class T>
    bool VisitAndPushdown(T* op)
    {
        YCHECK(op->Children().size() == 1);
        auto source = op->Children()[0];

        // TODO(sandello): Implement with explicit kinds.
        auto oldUnionOp = dynamic_cast<TUnionOperator*>(source);
        if (!oldUnionOp) {
            return true;
        }

        auto newUnionOp =
            new (Controller_->GetContext())
            TUnionOperator(Controller_->GetContext());

        for (auto& child : oldUnionOp->Children()) {
            auto clonedOp = op->Clone();
            clonedOp->AddChildNoCheck(child);
            newUnionOp->AddChild(clonedOp);
        }

        op->RemoveChild(oldUnionOp);
        op->AddChild(newUnionOp);
        op->Cut();

        if (*Head_ == op) {
            *Head_ = newUnionOp;
        }

        // We should stop traversal, because tree structure has changed.
        return false;
    }

    virtual bool Visit(TFilterOperator* op) override
    {
        return VisitAndPushdown(op);
    }

    virtual bool Visit(TProjectOperator* op) override
    {
        return VisitAndPushdown(op);
    }

private:
    TCoordinateController* Controller_;
    TOperator** Head_;

};

} // anonymous namespace

////////////////////////////////////////////////////////////////////////////////

IMegaReaderPtr TCoordinateController::Run()
{
    ViewFragment(Fragment_, "BeforeCoordinate");
    {
        TSplitFurther visitor(this);
        Traverse(&visitor, Fragment_.GetHead());
    }
    ViewFragment(Fragment_, "AfterSplitFurther");
    {
        TOperator* head = Fragment_.GetHead();
        TPushdown visitor(this, &head);
        while (!Traverse(&visitor, head));
        Fragment_.SetHead(head);
    }
    ViewFragment(Fragment_, "AfterPushdownFilter");
    return nullptr;
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

