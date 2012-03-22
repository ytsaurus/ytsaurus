#pragma once

#include "public.h"

#include <ytlib/misc/configurable.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct TMapOperationSpec
    : public TConfigurable
{
    Stroka ShellCommand;
    yvector<NYTree::TYPath> Files;
    yvector<NYTree::TYPath> In;
    yvector<NYTree::TYPath> Out;

    TMapOperationSpec()
    {
        SetKeepOptions(true);
        Register("shell_command", ShellCommand)
            .Default("");
        Register("files", Files)
            .Default(yvector<NYTree::TYPath>());
        Register("in", In);
        Register("out", Out);
    }
};

TAutoPtr<IOperationController> CreateMapController(
    IOperationHost* host,
    TOperation* operation);

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
