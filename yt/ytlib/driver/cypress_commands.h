#pragma once

#include "command.h"

#include <ytlib/ytree/public.h>
#include <ytlib/object_server/id.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

class TGetCommand
    : public TTransactedCommand
{
public:
    TGetCommand(IDriverImpl* driverImpl)
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

class TSetCommand
    : public TTransactedCommand
{
public:
    TSetCommand(IDriverImpl* driverImpl)
        : TTransactedCommand(driverImpl)
    {
        PathArg.Reset(new TFreeStringArg("path", "path in cypress", true, "", "string"));
        ValueArg.Reset(new TFreeStringArg("value", "value to set", true, "", "yson"));

        Cmd->add(~PathArg);
        Cmd->add(~ValueArg);
    }

    virtual void DoExecute();

private:
    THolder<TFreeStringArg> PathArg;
    THolder<TFreeStringArg> ValueArg;
};

////////////////////////////////////////////////////////////////////////////////

class TRemoveCommand
    : public TTransactedCommand
{
public:
    TRemoveCommand(IDriverImpl* driverImpl)
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

class TListCommand
    : public TTransactedCommand
{
public:
    TListCommand(IDriverImpl* driverImpl)
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

class TCreateCommand
    : public TTransactedCommand
{
public:
    TCreateCommand(IDriverImpl* driverImpl)
        : TTransactedCommand(driverImpl)
    {
        PathArg.Reset(new TFreeStringArg("path", "path in cypress", true, "", "string"));
        TypeArg.Reset(new TTypeArg(
            "type", "type of node", true, NObjectServer::EObjectType::Undefined, "object type"));

        Cmd->add(~PathArg);
        Cmd->add(~TypeArg);

        ManifestArg.Reset(new TManifestArg("", "manifest", "manifest", false, "", "yson"));
        Cmd->add(~ManifestArg);
    }

    virtual void DoExecute();

private:
    THolder<TFreeStringArg> PathArg;

    typedef TCLAP::UnlabeledValueArg<NObjectServer::EObjectType> TTypeArg;
    THolder<TTypeArg> TypeArg;

    typedef TCLAP::ValueArg<NYTree::TYson> TManifestArg;
    THolder<TManifestArg> ManifestArg;
};

////////////////////////////////////////////////////////////////////////////////

class TLockCommand
    : public TTransactedCommand
{
public:
    TLockCommand(IDriverImpl* driverImpl)
        : TTransactedCommand(driverImpl)
    {
        PathArg.Reset(new TFreeStringArg("path", "path in cypress", true, "", "string"));
        //TODO(panin): check given value
        ModeArg.Reset(new TModeArg(
            "", "mode", "lock mode", false, NCypress::ELockMode::Exclusive, "Snapshot, Shared, Exclusive"));
        Cmd->add(~PathArg);
        Cmd->add(~ModeArg);
    }

    virtual void DoExecute();

private:
    THolder<TFreeStringArg> PathArg;

    typedef TCLAP::ValueArg<NCypress::ELockMode> TModeArg;
    THolder<TModeArg> ModeArg;
};


////////////////////////////////////////////////////////////////////////////////


} // namespace NDriver
} // namespace NYT

