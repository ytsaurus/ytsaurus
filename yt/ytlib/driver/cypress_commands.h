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

    TGetRequest()
    {
        Register("path", Path);
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

    TSetRequest()
    {
        Register("path", Path);
        Register("value", Value);
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

