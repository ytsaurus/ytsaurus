#include "executor.h"

#include "private.h"

#include "coordinate_controller.h"
#include "evaluate_controller.h"

#include <ytlib/new_table_client/reader.h>
#include <ytlib/new_table_client/writer.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

class TEvaluatorProxy
    : public IExecutor
{
public:
    TEvaluatorProxy(IInvokerPtr invoker, IEvaluateCallbacks* callbacks)
        : Invoker_(std::move(invoker))
        , Callbacks_(callbacks)
    { }

    virtual TAsyncError Execute(
        const TPlanFragment& fragment,
        IWriterPtr writer) override
    {
        return BIND([=] () -> TError {
            TEvaluateController evaluator(Callbacks_, fragment, std::move(writer));
            return evaluator.Run();
        })
        .AsyncVia(Invoker_)
        .Run();
    }

private:
    IInvokerPtr Invoker_;
    IEvaluateCallbacks* Callbacks_;

};

IExecutorPtr CreateEvaluator(IInvokerPtr invoker, IEvaluateCallbacks* callbacks)
{
    return New<TEvaluatorProxy>(std::move(invoker), callbacks);
}

////////////////////////////////////////////////////////////////////////////////

class TCoordinatorProxy
    : public IExecutor
{
public:
    TCoordinatorProxy(IInvokerPtr invoker, ICoordinateCallbacks* callbacks)
        : Invoker_(std::move(invoker))
        , Callbacks_(callbacks)
    { }

    virtual TAsyncError Execute(
        const TPlanFragment& fragment,
        IWriterPtr writer) override
    {
        return BIND([=] () -> TError {
            TCoordinateController coordinator(Callbacks_, fragment);

            auto error = coordinator.Run();
            RETURN_IF_ERROR(error);

            TEvaluateController evaluator(
                &coordinator,
                coordinator.GetCoordinatorFragment(),
                std::move(writer));

            return evaluator.Run();
        })
        .AsyncVia(Invoker_)
        .Run();
    }

private:
    IInvokerPtr Invoker_;
    ICoordinateCallbacks* Callbacks_;

};

IExecutorPtr CreateCoordinator(IInvokerPtr invoker, ICoordinateCallbacks* callbacks)
{
    return New<TCoordinatorProxy>(std::move(invoker), callbacks);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

