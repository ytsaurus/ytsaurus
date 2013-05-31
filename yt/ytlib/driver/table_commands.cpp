#include "stdafx.h"
#include "table_commands.h"
#include "config.h"

#include <ytlib/misc/async_stream.h>

#include <ytlib/yson/parser.h>
#include <ytlib/yson/consumer.h>

#include <ytlib/ytree/tree_visitor.h>

#include <ytlib/transaction_client/transaction_manager.h>

#include <ytlib/table_client/table_reader.h>
#include <ytlib/table_client/table_writer.h>
#include <ytlib/table_client/table_consumer.h>
#include <ytlib/table_client/table_producer.h>

#include <ytlib/chunk_client/block_cache.h>

namespace NYT {
namespace NDriver {

using namespace NYson;
using namespace NYTree;
using namespace NFormats;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

class TReadSession
    : public TRefCounted
{
public:
    TReadSession(
        NTableClient::TAsyncTableReaderPtr reader,
        IAsyncOutputStreamPtr output,
        const TFormat& format,
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

TReadSession::TReadSession(
    TAsyncTableReaderPtr reader,
    IAsyncOutputStreamPtr output,
    const TFormat& format,
    size_t bufferLimit)
        : Reader_(reader)
        , Output_(output)
        , Consumer_(CreateConsumerForFormat(format, EDataType::Tabular, &Buffer_))
        , BufferSize_(bufferLimit)
        , AlreadyFetched_(false)
{ }

TAsyncError TReadSession::Execute()
{
    auto this_ = MakeStrong(this);
    return Reader_->AsyncOpen().Apply(BIND([this, this_] (TError error) -> TAsyncError {
        RETURN_FUTURE_IF_ERROR(error, TError);
        return this->Read();
    }));
}

TAsyncError TReadSession::Read()
{
    // Read rows synchronously and write them to the stream while possible.
    // AlreadyFetched_  is true if we'd fetched a row from the reader but didn't processed it yet.
    // Processed rows are stored in Buffer_ before being written to the output stream.
    // IsValid is true if the reader has more rows.
    auto this_ = MakeStrong(this);
    while (AlreadyFetched_ || Reader_->FetchNextItem()) {
        if (!Reader_->IsValid()) {
            return Output_->Write(Buffer_.Begin(), Buffer_.GetSize())
                ? MakeFuture(TError())
                : Output_->GetWriteFuture();
        }

        AlreadyFetched_ = false;

        try {
            // It is guaranteed that the reader contains a correct row.
            YCHECK(Reader_->IsValid());
            ProduceRow(~Consumer_, Reader_->GetRow(), Reader_->GetRowAttributes());
        } catch (const std::exception& ex) {
            return MakeFuture(TError(ex));
        }

        // NB: Consumer_ created on Buffer_, so after processing size of Buffer had changed.
        if (Buffer_.GetSize() > BufferSize_) {
            if (!Output_->Write(Buffer_.Begin(), Buffer_.GetSize())) {
                return Output_->GetWriteFuture().Apply(BIND([this, this_] (TError error) -> TAsyncError {
                    RETURN_FUTURE_IF_ERROR(error, TError);
                    Buffer_.Clear();
                    return this->Read();
                }));
            }
            else {
                Buffer_.Clear();
            }
        }
    }
    return Reader_->GetReadyEvent().Apply(BIND([this, this_] (TError error) -> TAsyncError {
        RETURN_FUTURE_IF_ERROR(error, TError);
        AlreadyFetched_ = true;
        return this->Read();
    }));
}

////////////////////////////////////////////////////////////////////////////////

class TWriteSession
    : public TRefCounted
{
public:
    TWriteSession(
        NTableClient::IAsyncWriterPtr writer,
        IAsyncInputStreamPtr input,
        const TFormat& format,
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

TWriteSession::TWriteSession(
    IAsyncWriterPtr writer,
    IAsyncInputStreamPtr input,
    const TFormat& format,
    i64 bufferSize)
        : Writer_(writer)
        , Input_(input)
        , Consumer_(new TTableConsumer(Writer_))
        , Parser_(CreateParserForFormat(format, EDataType::Tabular, ~Consumer_))
        , Buffer_(TSharedRef::Allocate(bufferSize))
        , IsFinished_(false)
{ }

TAsyncError TWriteSession::Execute()
{
    return Writer_->AsyncOpen().Apply(BIND(&TThis::ReadyToRead, MakeStrong(this)));
}

TAsyncError TWriteSession::ReadyToRead(TError error)
{
    RETURN_FUTURE_IF_ERROR(error, TError);
    return Read();
}

TAsyncError TWriteSession::Read()
{
    // Read data from the input stream synchronously, produce rows and push them into
    // the writer. When a certain stage cannot be finished synchronously we apply this method
    // to the corresponding future.
    auto this_ = MakeStrong(this);
    while (!IsFinished_ && Input_->Read(Buffer_.Begin(), Buffer_.Size())) {
        try {
            if (!ProcessReadResult()) {
                return Writer_->GetReadyEvent().Apply(BIND(&TThis::ReadyToRead, this_));
            }
        } catch (const std::exception& ex) {
            return MakeFuture(TError(ex));
        }
    }

    return IsFinished_
        ? Writer_->AsyncClose()
        : Input_->GetReadFuture().Apply(BIND(&TThis::OnRead, MakeStrong(this)));
}

bool TWriteSession::ProcessReadResult()
{
    if (Input_->GetReadLength() == 0) {
        IsFinished_ = true;
        Parser_->Finish();
    } else {
        Parser_->Read(TStringBuf(Buffer_.Begin(), Input_->GetReadLength()));
    }
    return Writer_->IsReady();
}

TAsyncError TWriteSession::OnRead(TError error)
{
    RETURN_FUTURE_IF_ERROR(error, TError);

    bool result;
    try {
        result = ProcessReadResult();
    } catch (const std::exception& ex) {
        return MakeFuture(TError(ex));
    }

    if (!result) {
        return Writer_->GetReadyEvent().Apply(
            BIND(&TThis::ReadyToRead, MakeStrong(this)));
    }

    return Read();
}

//////////////////////////////////////////////////////////////////////////////////

void TReadCommand::DoExecute()
{
    auto config = UpdateYsonSerializable(
        Context->GetConfig()->TableReader,
        Request->TableReader);

    auto reader = New<TAsyncTableReader>(
        config,
        Context->GetMasterChannel(),
        GetTransaction(EAllowNullTransaction::Yes, EPingTransaction::Yes),
        Context->GetBlockCache(),
        Request->Path);

    Session_ = New<TReadSession>(
        reader,
        Context->GetRequest()->OutputStream,
        Context->GetOutputFormat(),
        Context->GetConfig()->ReadBufferSize);

    CheckAndReply(Session_->Execute());
}

TReadCommand::TReadCommand()
{ }

TReadCommand::~TReadCommand()
{ }

//////////////////////////////////////////////////////////////////////////////////

void TWriteCommand::DoExecute()
{
    auto config = UpdateYsonSerializable(
        Context->GetConfig()->TableWriter,
        Request->TableWriter);

    auto writer = CreateAsyncTableWriter(
        config,
        Context->GetMasterChannel(),
        GetTransaction(EAllowNullTransaction::Yes, EPingTransaction::Yes),
        Context->GetTransactionManager(),
        Request->Path,
        Request->Path.Attributes().Find<TKeyColumns>("sorted_by"));

    Session_ = New<TWriteSession>(
        writer,
        Context->GetRequest()->InputStream,
        Context->GetInputFormat(),
        config->BlockSize);

    CheckAndReply(Session_->Execute());
}

TWriteCommand::TWriteCommand()
{ }

TWriteCommand::~TWriteCommand()
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
