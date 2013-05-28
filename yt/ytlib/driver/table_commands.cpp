#include "stdafx.h"
#include "table_commands.h"
#include "config.h"

#include <ytlib/misc/async_stream.h>

#include <ytlib/formats/format.h>

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

TReadSession::TReadSession(
    TAsyncTableReaderPtr reader,
    IAsyncOutputStreamPtr output,
    const TFormat& format,
    size_t bufferLimit)
        : Reader_(reader)
        , Output_(output)
        , Consumer_(CreateConsumerForFormat(format, EDataType::Tabular, &Buffer_))
        , BufferLimit_(bufferLimit)
        , AlreadyFetch_(false)
{ }

TAsyncError TReadSession::Execute()
{
    auto this_ = MakeStrong(this);
    return Reader_->AsyncOpen().Apply(BIND([this, this_] (TError error) {
        RETURN_FUTURE_IF_ERROR(error, TError);
        return Read();
    }));
}

TAsyncError TReadSession::Read()
{
    // Syncroniously reads rows and write it to stream while it is possible.
    // AlreadyFetch_ means that we we fetch row from the reader but did't process it yet.
    // Processed rows stored in the Buffer_ before writing it to the output stream.
    // IsValid marks that reader has more rows.
    auto this_ = MakeStrong(this);
    while (AlreadyFetch_ || Reader_->FetchNextItem()) {
        if (!Reader_->IsValid()) {
            return Output_->Write(Buffer_.Begin(), Buffer_.GetSize())
                ? MakeFuture(TError())
                : Output_->GetWriteFuture();
        }
        
        AlreadyFetch_ = false;
        
        try {
            // It is guaranteed that reader contains correct row
            YCHECK(Reader_->IsValid());
            ProduceRow(~Consumer_, Reader_->GetRow(), Reader_->GetRowAttributes());
        } catch (const std::exception& ex) {
            return MakeFuture(TError(ex));
        }

        // NB: Consumer_ created on Buffer_, so after processing size of Buffer had changed.
        if (Buffer_.GetSize() > BufferLimit_) {
            if (!Output_->Write(Buffer_.Begin(), Buffer_.GetSize())) {
                return Output_->GetWriteFuture().Apply(BIND([this, this_] (TError error) {
                    RETURN_FUTURE_IF_ERROR(error, TError);
                    Buffer_.Clear();
                    return Read();
                }));
            }
            else {
                Buffer_.Clear();
            }
        }
    }
    return Reader_->GetReadyEvent().Apply(BIND([this, this_] (TError error) {
        RETURN_FUTURE_IF_ERROR(error, TError);
        AlreadyFetch_ = true;
        return Read();
    }));
}

////////////////////////////////////////////////////////////////////////////////

TWriteSession::TWriteSession(
    IAsyncWriterPtr writer,
    IAsyncInputStreamPtr input,
    const TFormat& format,
    i64 bufferSize)
        : Writer_(writer)
        , Input_(input)
        , Consumer_(new TTableConsumer(Writer_))
        , Parser_(
            CreateParserForFormat(
                format,
                EDataType::Tabular,
                ~Consumer_))
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
    // Syncroniously reads data from the input stream, produce rows and push it to
    // the writer. When some operation couldn't be done syncroniously we apply this method 
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
    if (IsFinished_) {
        return Writer_->AsyncClose();
    } else {
        return Input_->GetReadFuture().Apply(BIND(&TThis::OnRead, MakeStrong(this)));
    }
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
        return Writer_->GetReadyEvent().Apply(BIND(&TThis::ReadyToRead, MakeStrong(this)));
    }
    return Read();
}


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

    auto driverRequest = Context->GetRequest();
    Session_ = New<TReadSession>(
        reader,
        driverRequest->OutputStream,
        ConvertTo<TFormat>(driverRequest->Arguments->FindChild("output_format")),
        Context->GetConfig()->ReadBufferSize);

    CheckAndReply(Session_->Execute());
}

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
    
    auto driverRequest = Context->GetRequest();
    Session_ = New<TWriteSession>(
        writer,
        driverRequest->InputStream,
        ConvertTo<TFormat>(driverRequest->Arguments->FindChild("input_format")),
        config->BlockSize);

    CheckAndReply(Session_->Execute());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
