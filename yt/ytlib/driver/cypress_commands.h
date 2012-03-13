#pragma once

#include "command.h"

#include <ytlib/ytree/public.h>
#include <ytlib/object_server/id.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

class TNewGetCommand
    : public TTransactedCommand
{
public:
    TNewGetCommand(IDriverImpl* driverImpl)
        : TTransactedCommand(driverImpl)
    {
        PathArg.Reset(new TFreeStringArg("path", "path in cypress", true, "", "string"));
        Cmd->add(~PathArg);
    }

    virtual void DoExecute(const yvector<Stroka>& args);

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

    virtual void DoExecute(const yvector<Stroka>& args);

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

    virtual void DoExecute(const yvector<Stroka>& args);

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

    virtual void DoExecute(const yvector<Stroka>& args);

private:
    THolder<TFreeStringArg> PathArg;
};

////////////////////////////////////////////////////////////////////////////////

struct TCreateRequest
    : public TTransactedRequest
{
    NYTree::TYPath Path;
    NYTree::INodePtr Stream;
    NObjectServer::EObjectType Type;
    NYTree::INodePtr Manifest;

    TCreateRequest()
    {
        Register("path", Path);
        Register("stream", Stream)
            .Default()
            .CheckThat(~StreamSpecIsValid);
        Register("type", Type);
        Register("manifest", Manifest)
            .Default();
    }
};

class TCreateCommand
    : public TCommandBase<TCreateRequest>
{
public:
    TCreateCommand(IDriverImpl* driverImpl)
        : TCommandBase(driverImpl)
    { }

private:
    virtual void DoExecute(TCreateRequest* request);
};

////////////////////////////////////////////////////////////////////////////////

struct TLockRequest
    : public TTransactedRequest
{
    NYTree::TYPath Path;
    NCypress::ELockMode Mode;

    TLockRequest()
    {
        Register("path", Path);
        Register("mode", Mode)
            .Default(NCypress::ELockMode::Exclusive);
    }
};

class TLockCommand
    : public TCommandBase<TLockRequest>
{
public:
    TLockCommand(IDriverImpl* driverImpl)
        : TCommandBase(driverImpl)
    { }

private:
    virtual void DoExecute(TLockRequest* request);
};

////////////////////////////////////////////////////////////////////////////////


} // namespace NDriver
} // namespace NYT

