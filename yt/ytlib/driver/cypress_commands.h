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
    Stroka Stream;

    TGetRequest()
    {
        Register("path", Path);
        Register("stream", Stream).Default(Stroka());
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
    Stroka Stream;

    TSetRequest()
    {
        Register("path", Path);
        Register("value", Value).Default(NULL);
        Register("stream", Stream).Default(Stroka());
    }

    virtual void Validate(const NYTree::TYPath& path = "/") const
    {
        TConfigBase::Validate(path);
        if (!Value && Stream.empty()) {
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

} // namespace NDriver
} // namespace NYT

