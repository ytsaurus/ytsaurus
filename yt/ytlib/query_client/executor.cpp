#include "executor.h"

#include "private.h"

#include "coordinate_controller.h"
#include "codegen_controller.h"

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

#ifndef YT_USE_LLVM

static TAsyncError GetQueriesNotSupportedErrror()
{
    return MakeFuture(TError("Query evaluation is not supported in this build"));
}

#endif

////////////////////////////////////////////////////////////////////////////////

class TEvaluatorProxy
    : public IExecutor
{
public:
    TEvaluatorProxy(IInvokerPtr invoker, IEvaluateCallbacks* callbacks)
        : Invoker_(std::move(invoker))
        , Callbacks_(callbacks)
#ifdef YT_USE_LLVM
        , CodegenController_(Invoker_)
#endif
    { }

    virtual TAsyncError Execute(
        const TPlanFragment& fragment,
        ISchemedWriterPtr writer) override
    {
#ifdef YT_USE_LLVM
        auto this_ = MakeStrong(this);
        return BIND([this, this_, fragment, writer] () -> TError {
                return CodegenController_.Run(Callbacks_, fragment, std::move(writer));
            })
            .AsyncVia(Invoker_)
            .Run();
#else
        return GetQueriesNotSupportedErrror();
#endif
    }

private:
    IInvokerPtr Invoker_;
    IEvaluateCallbacks* Callbacks_;
#ifdef YT_USE_LLVM
    TCodegenController CodegenController_;
#endif

};

////////////////////////////////////////////////////////////////////////////////

class TCoordinatorProxy
    : public IExecutor
{
public:
    TCoordinatorProxy(IInvokerPtr invoker, ICoordinateCallbacks* callbacks)
        : Invoker_(std::move(invoker))
        , Callbacks_(callbacks)
#ifdef YT_USE_LLVM
        , CodegenController_(Invoker_)
#endif
    { }

    virtual TAsyncError Execute(
        const TPlanFragment& fragment,
        ISchemedWriterPtr writer) override
    {
#ifdef YT_USE_LLVM
        auto this_ = MakeStrong(this);
        return BIND([this, this_, fragment, writer] () -> TError {
                TCoordinateController coordinator(Callbacks_, fragment);

                auto error = coordinator.Run();
                RETURN_IF_ERROR(error);

                return CodegenController_.Run(
                    &coordinator,
                    coordinator.GetCoordinatorFragment(),
                    std::move(writer));
            })
            .AsyncVia(Invoker_)
            .Run();
#else
        return GetQueriesNotSupportedErrror();
#endif
    }

private:
    IInvokerPtr Invoker_;
    ICoordinateCallbacks* Callbacks_;
#ifdef YT_USE_LLVM
    TCodegenController CodegenController_;
#endif

};

////////////////////////////////////////////////////////////////////////////////

IExecutorPtr CreateEvaluator(IInvokerPtr invoker, IEvaluateCallbacks* callbacks)
{
    return New<TEvaluatorProxy>(std::move(invoker), callbacks);
}

IExecutorPtr CreateCoordinator(IInvokerPtr invoker, ICoordinateCallbacks* callbacks)
{
    return New<TCoordinatorProxy>(std::move(invoker), callbacks);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

