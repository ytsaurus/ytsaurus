#pragma once

#include "public.h"
#include "plan_fragment.h"

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

class TPrepareController
{
public:
    TPrepareController(IPrepareCallbacks* callbacks, const Stroka& source);

    ~TPrepareController();

    TPlanFragment Run();

    IPrepareCallbacks* GetCallbacks()
    {
        return Callbacks_;
    }

    TPlanContext* GetContext()
    {
        return Context_.Get();
    }

private:
    void ParseSource();
    void GetInitialSplits();
    void CheckAndBindReferences();
    void TypecheckExpressions();
    void MoveAggregateExpressions();

    IPrepareCallbacks* Callbacks_;
    const Stroka& Source_;
    TPlanContextPtr Context_;
    const TOperator* Head_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

