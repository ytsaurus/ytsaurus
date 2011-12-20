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
    Stroka Out;

    TGetRequest()
    {
        Register("path", Path);
        Register("out", Out).Default(Stroka());
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
    Stroka In;

    TSetRequest()
    {
        Register("path", Path);
        Register("value", Value).Default(NULL);
        Register("in", In).Default(Stroka());
    }

    virtual void Validate(const NYTree::TYPath& path = "/") const
    {
        TConfigBase::Validate(path);
        if (!Value && In.empty()) {
            ythrow yexception() << Sprintf("Neither \"value\" nor \"in\" is specified (Path: %s)", ~path);
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

