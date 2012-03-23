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
    TNullable<int> JobCount;

    TMapOperationSpec()
    {
        SetKeepOptions(true);
        Register("shell_command", ShellCommand);
        Register("files", Files)
            .Default(yvector<NYTree::TYPath>());
        Register("in", In);
        Register("out", Out);
        // TODO(babenko): validate > 0
        Register("job_count", JobCount)
            .Default();
    }
};

IOperationControllerPtr CreateMapController(
    IOperationHost* host,
    TOperation* operation);

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
