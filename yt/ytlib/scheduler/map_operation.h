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

    virtual void DoValidate() const
    {
        if (ShellCommand.empty() && Files.empty()) {
            ythrow yexception() << "Neither \"shell_command\" nor \"files\" are given";
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
