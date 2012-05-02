#pragma once

#include "command.h"

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

struct TDownloadRequest
    : public TTransactedRequest
{
    NYTree::TYPath Path;

    TDownloadRequest()
    {
        Register("path", Path);
    }
};

typedef TIntrusivePtr<TDownloadRequest> TDownloadRequestPtr;

class TDownloadCommand
    : public TTypedCommandBase<TDownloadRequest>
{
public:
    explicit TDownloadCommand(ICommandHost* host)
        : TTypedCommandBase(host)
        , TUntypedCommandBase(host)
    { }

    virtual TCommandDescriptor GetDescriptor();

private:
    virtual void DoExecute(TDownloadRequestPtr request);
};

////////////////////////////////////////////////////////////////////////////////

struct TUploadRequest
    : public TTransactedRequest
{
    NYTree::TYPath Path;

    TUploadRequest()
    {
        Register("path", Path);
    }
};

typedef TIntrusivePtr<TUploadRequest> TUploadRequestPtr;

class TUploadCommand
    : public TTypedCommandBase<TUploadRequest>
{
public:
    explicit TUploadCommand(ICommandHost* host)
        : TTypedCommandBase(host)
        , TUntypedCommandBase(host)
    { }

    virtual TCommandDescriptor GetDescriptor();

private:
    virtual void DoExecute(TUploadRequestPtr request);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

