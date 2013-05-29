#pragma once

#include "command.h"

#include <ytlib/table_client/public.h>
#include <ytlib/table_client/sync_writer.h>

#include <ytlib/yson/consumer.h>
#include <ytlib/ypath/rich.h>

#include <ytlib/misc/blob_output.h>
#include <ytlib/misc/intrusive_ptr.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

class TReadSession
    : public TRefCounted
{
public:
    TReadSession(
        NTableClient::TAsyncTableReaderPtr reader,
        IAsyncOutputStreamPtr output,
        const NFormats::TFormat& format,
        size_t bufferLimit);

    TAsyncError Execute();

private:
    typedef TReadSession TThis;

    TAsyncError Read();

    NTableClient::TAsyncTableReaderPtr Reader_;
    IAsyncOutputStreamPtr Output_;
    TBlobOutput Buffer_;
    std::unique_ptr<NYson::IYsonConsumer> Consumer_;

    size_t BufferSize_;
    bool AlreadyFetched_;
};

struct TReadRequest
    : public TTransactionalRequest
{
    NYPath::TRichYPath Path;
    NYTree::INodePtr TableReader;

    TReadRequest()
    {
        RegisterParameter("path", Path);
        RegisterParameter("table_reader", TableReader)
            .Default(nullptr);
    }
};

typedef TIntrusivePtr<TReadRequest> TReadRequestPtr;

class TReadCommand
    : public TTypedCommand<TReadRequest>
{
private:
    virtual void DoExecute();

    TIntrusivePtr<TReadSession> Session_;
};

////////////////////////////////////////////////////////////////////////////////

class TWriteSession
    : public TRefCounted
{
public:
    TWriteSession(
        NTableClient::IAsyncWriterPtr writer,
        IAsyncInputStreamPtr input,
        const NFormats::TFormat& format,
        i64 bufferSize);

    TAsyncError Execute();

private:
    typedef TWriteSession TThis;

    TAsyncError ReadyToRead(TError error);
    TAsyncError Read();
    bool ProcessReadResult();
    TAsyncError OnRead(TError error);
    TAsyncError ProcessCollectedRows(TError error);
    TAsyncError Finish(TError error);

    NTableClient::IAsyncWriterPtr Writer_;
    IAsyncInputStreamPtr Input_;
    std::unique_ptr<NTableClient::TTableConsumer> Consumer_;
    std::unique_ptr<NFormats::IParser> Parser_;
    TSharedRef Buffer_;

    bool IsFinished_;
};

struct TWriteRequest
    : public TTransactionalRequest
{
    NYPath::TRichYPath Path;
    TNullable<NTableClient::TKeyColumns> SortedBy;
    NYTree::INodePtr TableWriter;

    TWriteRequest()
    {
        RegisterParameter("path", Path);
        RegisterParameter("table_writer", TableWriter)
            .Default(nullptr);
    }
};

typedef TIntrusivePtr<TWriteRequest> TWriteRequestPtr;

class TWriteCommand
    : public TTypedCommand<TWriteRequest>
{
private:
    virtual void DoExecute();

    TIntrusivePtr<TWriteSession> Session_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
