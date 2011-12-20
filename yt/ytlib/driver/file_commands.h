#pragma once

#include "command.h"

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

struct TReadFileRequest
    : TRequestBase
{
    NYTree::TYPath Path;
    Stroka Out;

    TReadFileRequest()
    {
        Register("path", Path);
        Register("out", Out).Default(Stroka());
    }
};

class TReadFileCommand
    : public TCommandBase<TReadFileRequest>
{
public:
    TReadFileCommand(IDriverImpl* driverImpl)
        : TCommandBase(driverImpl)
    { }

private:
    virtual void DoExecute(TReadFileRequest* request);
};

////////////////////////////////////////////////////////////////////////////////

struct TWriteFileRequest
    : TRequestBase
{
    NYTree::TYPath Path;
    Stroka In;

    TWriteFileRequest()
    {
        Register("path", Path);
        Register("in", In).Default(Stroka());
    }
};

class TWriteFileCommand
    : public TCommandBase<TWriteFileRequest>
{
public:
    TWriteFileCommand(IDriverImpl* driverImpl)
        : TCommandBase(driverImpl)
    { }

private:
    virtual void DoExecute(TWriteFileRequest* request);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

