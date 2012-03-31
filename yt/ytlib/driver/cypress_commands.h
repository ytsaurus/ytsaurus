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
    }
};

typedef TIntrusivePtr<TGetRequest> TGetRequestPtr;

class TGetCommand
    : public TCommandBase<TGetRequest>
{
public:
    TGetCommand(ICommandHost* commandHost)
        : TCommandBase(commandHost)
    { }

private:
    virtual void DoExecute(TGetRequestPtr request);
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
        Register("value", Value); //TODO(panin): Think about defaulting this value
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

typedef TIntrusivePtr<TSetRequest> TSetRequestPtr;

class TSetCommand
    : public TCommandBase<TSetRequest>
{
public:
    TSetCommand(ICommandHost* commandHost)
        : TCommandBase(commandHost)
    { }

private:
    virtual void DoExecute(TSetRequestPtr request);
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

typedef TIntrusivePtr<TRemoveRequest> TRemoveRequestPtr;

class TRemoveCommand
    : public TCommandBase<TRemoveRequest>
{
public:
    TRemoveCommand(ICommandHost* commandHost)
        : TCommandBase(commandHost)
    { }

private:
    virtual void DoExecute(TRemoveRequestPtr request);
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
    }
};

typedef TIntrusivePtr<TListRequest> TListRequestPtr;

class TListCommand
    : public TCommandBase<TListRequest>
{
public:
    TListCommand(ICommandHost* commandHost)
        : TCommandBase(commandHost)
    { }

private:
    virtual void DoExecute(TListRequestPtr request);
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
        Register("type", Type);
        Register("manifest", Manifest)
            .Default();
    }
};

typedef TIntrusivePtr<TCreateRequest> TCreateRequestPtr;

class TCreateCommand
    : public TCommandBase<TCreateRequest>
{
public:
    TCreateCommand(ICommandHost* commandHost)
        : TCommandBase(commandHost)
    { }

private:
    virtual void DoExecute(TCreateRequestPtr request);
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

typedef TIntrusivePtr<TLockRequest> TLockRequestPtr;

class TLockCommand
    : public TCommandBase<TLockRequest>
{
public:
    TLockCommand(ICommandHost* commandHost)
        : TCommandBase(commandHost)
    { }

private:
    virtual void DoExecute(TLockRequestPtr request);
};

////////////////////////////////////////////////////////////////////////////////


} // namespace NDriver
} // namespace NYT

