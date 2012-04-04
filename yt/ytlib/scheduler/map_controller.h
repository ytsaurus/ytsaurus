#pragma once

#include "public.h"
#include "config.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct TMapOperationSpec
    : public TOperationSpecBase
{
    Stroka Mapper;
    yvector<NYTree::TYPath> Files;
    yvector<NYTree::TYPath> In;
    yvector<NYTree::TYPath> Out;

    TMapOperationSpec()
    {
        Register("mapper", Mapper);
        Register("files", Files)
            .Default(yvector<NYTree::TYPath>());
        Register("in", In);
        Register("out", Out);
    }
};

IOperationControllerPtr CreateMapController(
    IOperationHost* host,
    TOperation* operation);

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
