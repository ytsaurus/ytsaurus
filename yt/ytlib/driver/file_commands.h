#pragma once

#include "command.h"

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

struct TDownloadRequest
    : public TRequestBase
{
    NYTree::TYPath Path;
    NYTree::INode::TPtr Stream;

    TDownloadRequest()
    {
        Register("path", Path);
        Register("stream", Stream).Default(NULL).CheckThat(~StreamSpecIsValid);
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
    NYTree::INode::TPtr Stream;

    TUploadRequest()
    {
        Register("path", Path);
        Register("stream", Stream).Default(NULL).CheckThat(~StreamSpecIsValid);
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

