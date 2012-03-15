#pragma once

#include "command.h"

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

class TUploadCommand
    : public TTransactedCommand
{
public:
    TUploadCommand(IDriverImpl* driverImpl)
        : TTransactedCommand(driverImpl)
    {
        PathArg.Reset(new TFreeStringArg("path", "path in cypress", true, "", "string"));
        Cmd->add(~PathArg);
    }

    virtual void DoExecute();

private:
    THolder<TFreeStringArg> PathArg;
};

////////////////////////////////////////////////////////////////////////////////

class TDownloadCommand
    : public TTransactedCommand
{
public:
    TDownloadCommand(IDriverImpl* driverImpl)
        : TTransactedCommand(driverImpl)
    {
        PathArg.Reset(new TFreeStringArg("path", "path in cypress", true, "", "string"));
        Cmd->add(~PathArg);
    }

    virtual void DoExecute();

private:
    THolder<TFreeStringArg> PathArg;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

