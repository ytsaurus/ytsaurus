#pragma once

#include "command.h"

#include <ytlib/misc/intrusive_ptr.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

struct TDownloadRequest
    : public TTransactionalRequest
{
    NYPath::TRichYPath Path;
    TNullable<i64> Offset;
    TNullable<i64> Length;
    NYTree::INodePtr FileReader;

    TDownloadRequest()
    {
        RegisterParameter("path", Path);
        RegisterParameter("offset", Offset)
            .Default(Null);
        RegisterParameter("length", Length)
            .Default(Null);
        RegisterParameter("file_reader", FileReader)
            .Default(nullptr);
    }
};

typedef TIntrusivePtr<TDownloadRequest> TDownloadRequestPtr;


class TDownloadSession;

class TDownloadCommand
    : public TTypedCommand<TDownloadRequest>
{
public:
    TDownloadCommand();

    ~TDownloadCommand();

private:
    virtual void DoExecute();

    TIntrusivePtr<TDownloadSession> Session_;
};

////////////////////////////////////////////////////////////////////////////////

struct TUploadRequest
    : public TTransactionalRequest
{
    NYPath::TRichYPath Path;
    NYTree::INodePtr FileWriter;

    TUploadRequest()
    {
        RegisterParameter("path", Path);
        RegisterParameter("file_writer", FileWriter)
            .Default(nullptr);
    }
};

typedef TIntrusivePtr<TUploadRequest> TUploadRequestPtr;


class TUploadSession;

class TUploadCommand
    : public TTypedCommand<TUploadRequest>
{
public:
    TUploadCommand();

    ~TUploadCommand();

private:
    virtual void DoExecute();

    TIntrusivePtr<TUploadSession> Session_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

