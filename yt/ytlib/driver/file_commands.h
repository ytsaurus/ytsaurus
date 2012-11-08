#pragma once

#include "command.h"

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

struct TDownloadRequest
    : public TTransactedRequest
{
    NYPath::TYPath Path;

    TDownloadRequest()
    {
        Register("path", Path);
    }
};

typedef TIntrusivePtr<TDownloadRequest> TDownloadRequestPtr;

class TDownloadCommand
    : public TTransactedCommandBase<TDownloadRequest>
{
public:
    explicit TDownloadCommand(ICommandContext* host)
        : TTransactedCommandBase(host)
        , TUntypedCommandBase(host)
    { }

private:
    virtual void DoExecute();
};

////////////////////////////////////////////////////////////////////////////////

struct TUploadRequest
    : public TTransactedRequest
{
    NYPath::TYPath Path;

    TUploadRequest()
    {
        Register("path", Path);
    }
};

typedef TIntrusivePtr<TUploadRequest> TUploadRequestPtr;

class TUploadCommand
    : public TTransactedCommandBase<TUploadRequest>
{
public:
    explicit TUploadCommand(ICommandContext* host)
        : TTransactedCommandBase(host)
        , TUntypedCommandBase(host)
    { }

private:
    virtual void DoExecute();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

