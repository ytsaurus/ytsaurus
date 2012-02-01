#pragma once

#include "command.h"

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

struct TMapRequest
    : public TRequestBase
{
    Stroka Command;
    yvector<NYTree::TYPath> Files;

    yvector<NYTree::TYPath> In;
    yvector<NYTree::TYPath> Out;

    TMapRequest()
    {
        Register("command", Command);
        Register("files", Files);
        Register("in", In);
        Register("out", Out);
    }
};

class TMapCommand
    : public TCommandBase<TMapRequest>
{
public:
    TMapCommand(IDriverImpl* driverImpl)
        : TCommandBase(driverImpl)
    { }

private:
    virtual void DoExecute(TMapRequest* request);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

