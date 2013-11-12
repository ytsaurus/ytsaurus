#pragma once

#include "public.h"
#include "callbacks.h"
#include "plan_fragment.h"

#include <core/logging/tagged_logger.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

class TEvaluateController
    : public TRefCounted
{
public:
    TEvaluateController(
        IEvaluateCallbacks* callbacks,
        const TPlanFragment& fragment,
        IWriterPtr writer);

    ~TEvaluateController();

    TError Run();

    IEvaluateCallbacks* GetCallbacks()
    {
        return Callbacks_;
    }

    TPlanContext* GetContext()
    {
        return Fragment_.GetContext().Get();
    }

    const TOperator* GetHead()
    {
        return Fragment_.GetHead();
    }

private:
    // XXX(sandello): Temporary functions; to be codegen'd.
    TError RunUnion();
    TError RunProject();

    void SetHead(const TOperator* head)
    {
        Fragment_.SetHead(head);
    }

    template <class TFunctor>
    void Rewrite(const TFunctor& functor)
    {
        SetHead(Apply(GetContext(), GetHead(), functor));
    }

private:
    IEvaluateCallbacks* Callbacks_;
    TPlanFragment Fragment_;
    IWriterPtr Writer_;

    NLog::TTaggedLogger Logger;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

