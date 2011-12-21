#pragma once

#include "command.h"

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

struct TDownloadRequest
    : TRequestBase
{
    NYTree::TYPath Path;
    Stroka Stream;

    TDownloadRequest()
    {
        Register("path", Path);
        Register("stream", Stream).Default(Stroka());
    }
};

class TDownloadCommand
    : public TCommandBase<TDownloadRequest>
{
public:
    TDownloadCommand(IDriverImpl* driverImpl)
        : TCommandBase(driverImpl)
    { }

private:
    virtual void DoExecute(TDownloadRequest* request);
};

////////////////////////////////////////////////////////////////////////////////

struct TUploadRequest
    : TRequestBase
{
    NYTree::TYPath Path;
    Stroka Stream;

    TUploadRequest()
    {
        Register("path", Path);
        Register("stream", Stream).Default(Stroka());
    }
};

class TUploadCommand
    : public TCommandBase<TUploadRequest>
{
public:
    TUploadCommand(IDriverImpl* driverImpl)
        : TCommandBase(driverImpl)
    { }

private:
    virtual void DoExecute(TUploadRequest* request);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

