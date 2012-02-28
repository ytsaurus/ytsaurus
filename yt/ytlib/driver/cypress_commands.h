#pragma once

#include "command.h"

#include <ytlib/ytree/public.h>
#include <ytlib/object_server/id.h>

namespace NYT {
namespace NDriver {
    
////////////////////////////////////////////////////////////////////////////////

struct TGetRequest
    : public TTransactedRequest
{
    NYTree::TYPath Path;
    NYTree::INodePtr Stream;

    TGetRequest()
    {
        Register("path", Path);
        Register("stream", Stream)
            .Default()
            .CheckThat(~StreamSpecIsValid);
    }
};

class TGetCommand
    : public TCommandBase<TGetRequest>
{
public:
    TGetCommand(IDriverImpl* driverImpl)
        : TCommandBase(driverImpl)
    { }

private:
    virtual void DoExecute(TGetRequest* request);
};

////////////////////////////////////////////////////////////////////////////////

struct TSetRequest
    : public TTransactedRequest
{
    NYTree::TYPath Path;
    NYTree::INodePtr Value;
    NYTree::INodePtr Stream;

    TSetRequest()
    {
        Register("path", Path);
        Register("value", Value)
            .Default();
        Register("stream", Stream)
            .Default()
            .CheckThat(~StreamSpecIsValid);
    }

    virtual void DoValidate() const
    {
        if (!Value && !Stream) {
            ythrow yexception() << Sprintf("Neither \"value\" nor \"stream\" is given");
        }
        if (Value && Stream) {
            ythrow yexception() << Sprintf("Both \"value\" and \"stream\" are given");
        }
    }
};

class TSetCommand
    : public TCommandBase<TSetRequest>
{
public:
    TSetCommand(IDriverImpl* driverImpl)
        : TCommandBase(driverImpl)
    { }

private:
    virtual void DoExecute(TSetRequest* request);
};

////////////////////////////////////////////////////////////////////////////////

struct TRemoveRequest
    : public TTransactedRequest
{
    NYTree::TYPath Path;

    TRemoveRequest()
    {
        Register("path", Path);
    }
};

class TRemoveCommand
    : public TCommandBase<TRemoveRequest>
{
public:
    TRemoveCommand(IDriverImpl* driverImpl)
        : TCommandBase(driverImpl)
    { }

private:
    virtual void DoExecute(TRemoveRequest* request);
};

////////////////////////////////////////////////////////////////////////////////

struct TListRequest
    : public TTransactedRequest
{
    NYTree::TYPath Path;
    NYTree::INodePtr Stream;

    TListRequest()
    {
        Register("path", Path);
        Register("stream", Stream)
            .Default()
            .CheckThat(~StreamSpecIsValid);
    }
};

class TListCommand
    : public TCommandBase<TListRequest>
{
public:
    TListCommand(IDriverImpl* driverImpl)
        : TCommandBase(driverImpl)
    { }

private:
    virtual void DoExecute(TListRequest* request);
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

