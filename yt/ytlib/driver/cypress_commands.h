#pragma once

#include "command.h"

#include "../ytree/ytree.h"

namespace NYT {
namespace NDriver {
    
////////////////////////////////////////////////////////////////////////////////

struct TGetRequest
    : TRequestBase
{
    NYTree::TYPath Path;
    NYTree::INode::TPtr Stream;

    TGetRequest()
    {
        Register("path", Path);
        Register("stream", Stream).Default(NULL).CheckThat(~StreamSpecIsValid);
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
    : TRequestBase
{
    NYTree::TYPath Path;
    NYTree::INode::TPtr Value;
    NYTree::INode::TPtr Stream;

    TSetRequest()
    {
        Register("path", Path);
        Register("value", Value).Default(NULL);
        Register("stream", Stream).Default(NULL).CheckThat(~StreamSpecIsValid);
    }

    virtual void Validate(const NYTree::TYPath& path = "/") const
    {
        TConfigurable::Validate(path);
        if (!Value && !Stream) {
            ythrow yexception() << Sprintf("Neither \"value\" nor \"stream\" is specified (Path: %s)", ~path);
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
    : TRequestBase
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
    : TRequestBase
{
    NYTree::TYPath Path;
    NYTree::INode::TPtr Stream;

    TListRequest()
    {
        Register("path", Path);
        Register("stream", Stream).Default(NULL).CheckThat(~StreamSpecIsValid);
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
    : TRequestBase
{
    NYTree::TYPath Path;
    NYTree::INode::TPtr Stream;
    Stroka Type;
    NYTree::TYson Manifest;

    TCreateRequest()
    {
        Register("path", Path);
        Register("stream", Stream).Default(NULL).CheckThat(~StreamSpecIsValid);
        Register("type", Type);
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

} // namespace NDriver
} // namespace NYT

