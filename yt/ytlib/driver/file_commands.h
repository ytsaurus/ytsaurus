#pragma once

#include "command.h"

#include <ytlib/file_client/file_reader.h>
#include <ytlib/file_client/file_writer.h>

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


class TDownloadSession
    : public TRefCounted
{
public:
    TDownloadSession(NFileClient::TAsyncReaderPtr reader, IAsyncOutputStreamPtr output);

    TAsyncError Execute(
        NFileClient::TFileReaderConfigPtr config,
        NRpc::IChannelPtr masterChannel,
        NTransactionClient::ITransactionPtr transaction,
        NChunkClient::IBlockCachePtr blockCache,
        const NYPath::TRichYPath& richPath,
        const TNullable<i64>& offset,
        const TNullable<i64>& length);

private:
    typedef TDownloadSession TThis;

    TAsyncError ReadBlock(TError error);
    
    TAsyncError WriteBlock(TValueOrError<TSharedRef> blockOrError);

    NFileClient::TAsyncReaderPtr Reader_;
    IAsyncOutputStreamPtr Output_;
};

typedef TIntrusivePtr<TDownloadSession> TDownloadSessionPtr;


class TDownloadCommand
    : public TTypedCommand<TDownloadRequest>
{
private:
    virtual void DoExecute();

    TDownloadSessionPtr Session_;
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


class TUploadSession
    : public TRefCounted
{
public:
    TUploadSession(
        NFileClient::TAsyncWriterPtr writer,
        IAsyncInputStreamPtr input,
        size_t blockSize);

    TAsyncError Execute();

private:
    typedef TUploadSession TThis;

    TAsyncError ReadBlock(TError error);
    
    TAsyncError WriteBlock(TError error);

    NFileClient::TAsyncWriterPtr Writer_;
    IAsyncInputStreamPtr Input_;
    TSharedRef Buffer_;
};

typedef TIntrusivePtr<TUploadSession> TUploadSessionPtr;


class TUploadCommand
    : public TTypedCommand<TUploadRequest>
{
private:
    virtual void DoExecute();

    TUploadSessionPtr Session_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

