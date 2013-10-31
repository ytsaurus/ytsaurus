#pragma once

#include "public.h"
#include "callbacks.h"
#include "query_fragment.h"

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
        const TQueryFragment& fragment,
        TWriterPtr writer);

    ~TEvaluateController();

    TError Run();

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
    TWriterPtr Writer_;

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

