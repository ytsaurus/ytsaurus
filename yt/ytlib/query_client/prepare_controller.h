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

    void ParseSource();
    void GetInitialSplits();
    void CheckAndPruneReferences();
    void TypecheckExpressions();

    IPrepareCallbacks* GetCallbacks()
    {
        return Callbacks_;
    }

    TPlanContext* GetContext()
    {
        return Context_.Get();
    }

private:
    IPrepareCallbacks* Callbacks_;
    const Stroka& Source_;
    TPlanContextPtr Context_;
    const TOperator* Head_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

