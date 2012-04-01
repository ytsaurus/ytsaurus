#pragma once

#include "public.h"
#include "config.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct TMergeOperationSpec
    : public TOperationSpecBase
{
    yvector<NYTree::TYPath> In;
    NYTree::TYPath Out;
    bool Sorted;

    TMergeOperationSpec()
    {
        Register("in", In);
        Register("out", Out);
        Register("sorted", Sorted)
            .Default(false);
    }
};

IOperationControllerPtr CreateMergeController(
    IOperationHost* host,
    TOperation* operation);

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
