#pragma once

#include "public.h"
#include "plan_fragment.h"

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

class TPrepareController
{
public:
    TPrepareController(
        IPrepareCallbacks* callbacks,
        const Stroka& source,
        i64 inputRowLimit,
        i64 outputRowLimit,
        TTimestamp timestamp);

    ~TPrepareController();

    TPlanFragment Run();

private:
    void ParseSource();
    void CheckDepth();
    void GetInitialSplits();
    void CheckAndPruneReferences();
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

