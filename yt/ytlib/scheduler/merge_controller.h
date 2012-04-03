#pragma once

#include "public.h"
#include "config.h"

#include <ytlib/misc/enum.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(EMergeMode,
    (Sorted)
    (Ordered)
    (Unordered)
);

struct TMergeOperationSpec
    : public TOperationSpecBase
{
    yvector<NYTree::TYPath> In;
    NYTree::TYPath Out;
    EMergeMode Mode;
    bool CombineChunks;

    TMergeOperationSpec()
    {
        Register("in", In);
        Register("out", Out);
        Register("mode", Mode)
            .Default(EMergeMode::Unordered);
        Register("combine_chunks", CombineChunks)
            .Default(false);
    }
};

IOperationControllerPtr CreateMergeController(
    IOperationHost* host,
    TOperation* operation);

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
