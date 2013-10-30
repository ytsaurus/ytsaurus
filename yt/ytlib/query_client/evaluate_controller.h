#pragma once

#include "public.h"

#include "query_fragment.h"

#include <core/logging/tagged_logger.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

class TEvaluateController
{
public:
    TEvaluateController(
        IEvaluateCallbacks* callbacks,
        const TQueryFragment& fragment);

    ~TEvaluateController();

    TError Run(TWriterPtr writer);

    IEvaluateCallbacks* GetCallbacks()
    {
        return Callbacks_;
    }

    TQueryContext* GetContext()
    {
        return Fragment_.GetContext().Get();
    }

    const TOperator* GetHead()
    {
        return Fragment_.GetHead();
    }

private:
    IEvaluateCallbacks* Callbacks_;
    TQueryFragment Fragment_;

    void SetHead(const TOperator* head)
    {
        Fragment_.SetHead(head);
    }

    template <class TFunctor>
    void Rewrite(const TFunctor& functor)
    {
        SetHead(Apply(GetContext(), GetHead(), functor));
    }

    NLog::TTaggedLogger Logger;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

