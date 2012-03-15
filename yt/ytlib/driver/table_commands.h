#pragma once

#include "command.h"

#include <ytlib/ytree/public.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

class TReadCommand
    : public TTransactedCommand
{
public:
    TReadCommand(IDriverImpl* driverImpl)
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

class TWriteCommand
    : public TTransactedCommand
{
public:
    TWriteCommand(IDriverImpl* driverImpl)
        : TTransactedCommand(driverImpl)
    {
        PathArg.Reset(new TFreeStringArg("path", "path in cypress", true, "", "string"));
        Cmd->add(~PathArg);

        ValueArg.Reset(new TFreeStringArg("value", "value to set", true, "", "yson"));
        Cmd->add(~ValueArg);
    }

    // TODO(panin): validation?
//    virtual void DoValidate() const
//    {
//        if (Value) {
//            auto type = Value->GetType();
//            if (type != NYTree::ENodeType::List && type != NYTree::ENodeType::Map) {
//                ythrow yexception() << "\"value\" must be a list or a map";
//            }
//        }
//    }

    virtual void DoExecute();

private:
    THolder<TFreeStringArg> PathArg;

    //TODO(panin):support value from stdin
    THolder<TFreeStringArg> ValueArg;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

