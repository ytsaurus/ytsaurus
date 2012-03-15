#pragma once

#include "command.h"

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

class TStartCommand
    : public TNewCommandBase
{
public:
    TStartCommand(IDriverImpl* driverImpl)
        : TNewCommandBase(driverImpl)
    {
        ManifestArg.Reset(new TManifestArg("", "manifest", "manifest", false, "", "yson"));
        Cmd->add(~ManifestArg);
    }

    virtual void DoExecute();

private:
    typedef TCLAP::ValueArg<NYTree::TYson> TManifestArg;
    THolder<TManifestArg> ManifestArg;
};

////////////////////////////////////////////////////////////////////////////////

class TCommitCommand
    : public TTransactedCommand
{
    TCommitCommand(IDriverImpl* driverImpl)
        : TTransactedCommand(driverImpl)
    { }

    virtual void DoExecute();
};

////////////////////////////////////////////////////////////////////////////////

class TAbortCommand
    : public TTransactedCommand
{
    TAbortCommand(IDriverImpl* driverImpl)
        : TTransactedCommand(driverImpl)
    { }

    virtual void DoExecute();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

