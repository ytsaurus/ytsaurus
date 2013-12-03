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
                return TEvaluateController(Callbacks_, fragment, std::move(writer))
                    .Run();            
            })
            .AsyncVia(Invoker_)
            .Run();
    }

private:
    IInvokerPtr Invoker_;
    IEvaluateCallbacks* Callbacks_;
};

class TCoordinatedEvaluatorProxy
    : public IExecutor
{
public:
    TCoordinatedEvaluatorProxy(IInvokerPtr invoker, ICoordinateCallbacks* callbacks)
        : Invoker_(std::move(invoker))
        , Callbacks_(callbacks)
    { }

    virtual TAsyncError Execute(
        const TPlanFragment& fragment,
        IWriterPtr writer) override
    {
        return BIND([=] () -> TError {
                TCoordinateController coordinator(Callbacks_, fragment);

                auto result = coordinator.Run();

                if (result.IsOK()) {
                    TEvaluateController evaluator(
                        &coordinator, 
                        coordinator.GetCoordinatorSplit(), 
                        std::move(writer));

                    return evaluator.Run();
                } else {
                    return result;
                }
            })
            .AsyncVia(Invoker_)
            .Run();
    }

private:
    IInvokerPtr Invoker_;
    ICoordinateCallbacks* Callbacks_;
};

////////////////////////////////////////////////////////////////////////////////

IExecutorPtr CreateEvaluator(IInvokerPtr invoker, IEvaluateCallbacks* callbacks)
{
    return New<TEvaluatorProxy>(std::move(invoker), callbacks);
}

IExecutorPtr CreateCoordinatedEvaluator(IInvokerPtr invoker, ICoordinateCallbacks* callbacks)
{
    return New<TCoordinatedEvaluatorProxy>(std::move(invoker), callbacks);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

