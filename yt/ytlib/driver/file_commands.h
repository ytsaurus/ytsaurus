#pragma once

#include "command.h"

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

struct TDownloadRequest
    : public TTransactedRequest
{
    NYPath::TRichYPath Path;

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
    { }

private:
    virtual void DoExecute();
};

////////////////////////////////////////////////////////////////////////////////

struct TUploadRequest
    : public TTransactedRequest
{
    NYPath::TRichYPath Path;
    NYTree::INodePtr Attributes;

    TUploadRequest()
    {
        Register("path", Path);
        Register("attributes", Attributes)
            .Default(NULL);
    }
};

typedef TIntrusivePtr<TUploadRequest> TUploadRequestPtr;

class TUploadCommand
    : public TTransactedCommandBase<TUploadRequest>
{
public:
    explicit TUploadCommand(ICommandContext* host)
        : TTransactedCommandBase(host)
    { }

private:
    virtual void DoExecute();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

