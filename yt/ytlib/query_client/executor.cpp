#include "stdafx.h"
#include "executor.h"

#include "private.h"

#include "coordinator.h"
#include "evaluator.h"

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

#ifndef YT_USE_LLVM

static TFuture<TErrorOr<TQueryStatistics>> GetQueriesNotSupportedError()
{
    return MakeFuture(TErrorOr<TQueryStatistics>(TError("Query evaluation is not supported in this build")));
}

#endif

////////////////////////////////////////////////////////////////////////////////

class TEvaluatorProxy
    : public IExecutor
{
public:
    TEvaluatorProxy(
        TExecutorConfigPtr config,
        IInvokerPtr invoker,
        IEvaluateCallbacks* callbacks)
        : Invoker_(std::move(invoker))
        , Callbacks_(callbacks)
        , Evaluator_(std::move(config))
    { }

    virtual TFuture<TErrorOr<TQueryStatistics>> Execute(
        const TPlanFragmentPtr& fragment,
        ISchemafulWriterPtr writer) override
    {
#ifdef YT_USE_LLVM
        auto this_ = MakeStrong(this);
        return BIND([this, this_, fragment, writer] () {
                return Evaluator_.Run(Callbacks_, fragment, std::move(writer));
            })
            .Guarded()
            .AsyncVia(Invoker_)
            .Run();
#else
        return GetQueriesNotSupportedError();
#endif
    }

private:
    IInvokerPtr Invoker_;
    IEvaluateCallbacks* Callbacks_;

#ifdef YT_USE_LLVM
    TEvaluator Evaluator_;
#endif

};

IExecutorPtr CreateEvaluator(
    TExecutorConfigPtr config,
    IInvokerPtr invoker,
    IEvaluateCallbacks* callbacks)
{
    return New<TEvaluatorProxy>(
        std::move(config),
        std::move(invoker),
        callbacks);
}

////////////////////////////////////////////////////////////////////////////////

class TCoordinatorProxy
    : public IExecutor
{
public:
    TCoordinatorProxy(
        TExecutorConfigPtr config,
        IInvokerPtr invoker,
        ICoordinateCallbacks* callbacks)
        : Invoker_(std::move(invoker))
        , Callbacks_(callbacks)
        , Evaluator_(std::move(config))
    { }

    virtual TFuture<TErrorOr<TQueryStatistics>> Execute(
        const TPlanFragmentPtr& fragment,
        ISchemafulWriterPtr writer) override
    {
#ifdef YT_USE_LLVM
        auto this_ = MakeStrong(this);

        return BIND([this, this_, fragment, writer] () -> TQueryStatistics {
                TCoordinator coordinator(Callbacks_, fragment);
                coordinator.Run();

                auto result = Evaluator_.Run(
                    &coordinator,
                    coordinator.GetCoordinatorFragment(),
                    std::move(writer));
                result += coordinator.GetStatistics();
            
                return result;
            })
            .Guarded()
            .AsyncVia(Invoker_)
            .Run();
#else
        return GetQueriesNotSupportedError();
#endif
    }

private:
    IInvokerPtr Invoker_;
    ICoordinateCallbacks* Callbacks_;

#ifdef YT_USE_LLVM
    TEvaluator Evaluator_;
#endif

};

IExecutorPtr CreateCoordinator(
    TExecutorConfigPtr config,
    IInvokerPtr invoker,
    ICoordinateCallbacks* callbacks)
{
    return New<TCoordinatorProxy>(
        std::move(config),
        std::move(invoker),
        callbacks);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

