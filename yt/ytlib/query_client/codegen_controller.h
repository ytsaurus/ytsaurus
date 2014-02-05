#pragma once

#include "public.h"
#include "callbacks.h"
#include "plan_fragment.h"

namespace NYT {
namespace NQueryClient {

class TCodegenControllerImpl;

class TCodegenController
    : public NNonCopyable::TNonCopyable
{
public:
    explicit TCodegenController(IInvokerPtr invoker);

    ~TCodegenController();

    TError Run(
        IEvaluateCallbacks* callbacks,
        const TPlanFragment& fragment,
        ISchemedWriterPtr writer);

private:
    std::unique_ptr<TCodegenControllerImpl> Impl;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

