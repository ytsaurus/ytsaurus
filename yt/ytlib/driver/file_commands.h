#pragma once

#include "command.h"

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

struct TDownloadRequest
    : public TTransactionalRequest
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
    : public TTypedCommandBase<TDownloadRequest>
    , public TTransactionalCommandMixin
{
public:
    explicit TDownloadCommand(ICommandContext* context)
        : TTypedCommandBase(context)
        , TTransactionalCommandMixin(context, Request)
    { }

private:
    virtual void DoExecute();
};

////////////////////////////////////////////////////////////////////////////////

struct TUploadRequest
    : public TTransactionalRequest
{
    NYPath::TRichYPath Path;
    NYTree::INodePtr Attributes;

    TUploadRequest()
    {
        RegisterParameter("path", Path);
        RegisterParameter("attributes", Attributes)
            .Default(nullptr);
    }
};

typedef TIntrusivePtr<TUploadRequest> TUploadRequestPtr;

class TUploadCommand
    : public TTypedCommandBase<TUploadRequest>
    , public TTransactionalCommandMixin
{
public:
    explicit TUploadCommand(ICommandContext* context)
        : TTypedCommandBase(context)
        , TTransactionalCommandMixin(context, Request)
    { }

private:
    virtual void DoExecute();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

