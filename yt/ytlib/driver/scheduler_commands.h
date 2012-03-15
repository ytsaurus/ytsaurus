#pragma once

#include "command.h"

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

//TODO(panin): move to new interface
//struct TMapRequest
//    : public TRequestBase
//{
//    Stroka Command;
//    yvector<NYTree::TYPath> Files;

//    yvector<NYTree::TYPath> In;
//    yvector<NYTree::TYPath> Out;

//    TMapRequest()
//    {
//        Register("command", Command)
//            .Default(Stroka());
//        Register("files", Files)
//            .Default(yvector<NYTree::TYPath>());
//        Register("in", In);
//        Register("out", Out);
//    }

//    virtual void DoValidate() const
//    {
//        if (Command.empty() && Files.empty()) {
//            ythrow yexception() << "Neither \"command\" nor \"files\" are given";
//        }
//    }
//};

//class TMapCommand
//    : public TCommandBase<TMapRequest>
//{
//public:
//    TMapCommand(IDriverImpl* driverImpl)
//        : TCommandBase(driverImpl)
//    { }

//private:
//    virtual void DoExecute(TMapRequest* request);
//};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

