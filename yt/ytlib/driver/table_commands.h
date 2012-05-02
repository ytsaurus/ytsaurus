#pragma once

#include "command.h"

#include <ytlib/ytree/public.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

struct TReadRequest
    : public TTransactedRequest
{
    NYTree::TYPath Path;
    NYTree::INodePtr Stream;

    TReadRequest()
    {
        Register("path", Path);
    }
};

typedef TIntrusivePtr<TReadRequest> TReadRequestPtr;

class TReadCommand
    : public TTypedCommandBase<TReadRequest>
{
public:
    explicit TReadCommand(ICommandHost* host)
        : TTypedCommandBase(host)
        , TUntypedCommandBase(host)
    { }

    virtual TCommandDescriptor GetDescriptor();

private:
    virtual void DoExecute(TReadRequestPtr request);
};

////////////////////////////////////////////////////////////////////////////////

struct TWriteRequest
    : public TTransactedRequest
{
    NYTree::TYPath Path;
    NYTree::INodePtr Stream;
    NYTree::INodePtr Value;
    bool Sorted;

    TWriteRequest()
    {
        Register("path", Path);
        Register("value", Value)
            .Default();
        Register("sorted", Sorted)
            .Default(false);
    }

    virtual void DoValidate() const
    {
        if (Value) {
            auto type = Value->GetType();
            if (type != NYTree::ENodeType::List && type != NYTree::ENodeType::Map) {
                ythrow yexception() << "\"value\" must be a list or a map";
            }
        }
    }
};

typedef TIntrusivePtr<TWriteRequest> TWriteRequestPtr;

class TWriteCommand
    : public TTypedCommandBase<TWriteRequest>
{
public:
    explicit TWriteCommand(ICommandHost* host)
        : TTypedCommandBase(host)
        , TUntypedCommandBase(host)
    { }

    virtual TCommandDescriptor GetDescriptor();

private:
    virtual void DoExecute(TWriteRequestPtr request);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

