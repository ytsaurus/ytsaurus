#include "executor.h"

#include "private.h"

#include "coordinate_controller.h"
#include "evaluate_controller.h"

#include <ytlib/new_table_client/writer.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

template <class TController, class TCallbacks>
class TControlledExecutor
    : public IExecutor
{
public:
    TControlledExecutor(IInvokerPtr invoker, TCallbacks* callbacks)
        : Invoker_(std::move(invoker))
        , Callbacks_(callbacks)
    { }

    virtual TAsyncError Execute(
        const TPlanFragment& fragment,
        IWriterPtr writer) override
    {
        auto controller = New<TController>(
            Callbacks_,
            fragment,
            std::move(writer));
        return BIND(&TController::Run, controller)
            .AsyncVia(Invoker_)
            .Run();
    }

private:
    IInvokerPtr Invoker_;
    TCallbacks* Callbacks_;

};

////////////////////////////////////////////////////////////////////////////////

IExecutorPtr CreateEvaluator(IInvokerPtr invoker, IEvaluateCallbacks* callbacks)
{
    typedef TControlledExecutor<TEvaluateController, IEvaluateCallbacks> TExecutor;
    return New<TExecutor>(std::move(invoker), callbacks);
}

IExecutorPtr CreateCoordinator(IInvokerPtr invoker, ICoordinateCallbacks* callbacks)
{
    typedef TControlledExecutor<TCoordinateController, ICoordinateCallbacks> TExecutor;
    return New<TExecutor>(std::move(invoker), callbacks);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

