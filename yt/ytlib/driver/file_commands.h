#pragma once

#include "command.h"

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

struct TDownloadRequest
    : public TTransactedRequest
{
    NYPath::TRichYPath Path;
    TNullable<i64> Offset;
    TNullable<i64> Length;

    TDownloadRequest()
    {
        RegisterParameter("path", Path);
        RegisterParameter("offset", Offset)
            .Default(Null);
        RegisterParameter("length", Length)
            .Default(Null);
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
        RegisterParameter("path", Path);
        RegisterParameter("attributes", Attributes)
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

