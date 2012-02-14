#pragma once

#include "command.h"

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

struct TDownloadRequest
    : public TRequestBase
{
    NYTree::TYPath Path;
    NYTree::TNodePtr Stream;

    TDownloadRequest()
    {
        Register("path", Path);
        Register("stream", Stream)
            .Default()
            .CheckThat(~StreamSpecIsValid);
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
    : public TRequestBase
{
    NYTree::TYPath Path;
    NYTree::TNodePtr Stream;

    TUploadRequest()
    {
        Register("path", Path);
        Register("stream", Stream)
            .Default()
            .CheckThat(~StreamSpecIsValid);
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

