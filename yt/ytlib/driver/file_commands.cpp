#include "stdafx.h"
#include "file_commands.h"
#include "config.h"
#include "driver.h"

#include <ytlib/ytree/fluent.h>
#include <ytlib/transaction_client/transaction_manager.h>
#include <ytlib/file_client/file_reader.h>
#include <ytlib/file_client/file_writer.h>

namespace NYT {
namespace NDriver {

using namespace NFileClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

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

    TAsyncError WriteBlock(TErrorOr<TSharedRef> blockOrError);

    NFileClient::TAsyncReaderPtr Reader_;
    IAsyncOutputStreamPtr Output_;
};


TDownloadSession::TDownloadSession(TAsyncReaderPtr reader, IAsyncOutputStreamPtr output)
    : Reader_(reader)
    , Output_(output)
{ }

TAsyncError TDownloadSession::Execute(
    TFileReaderConfigPtr config,
    NRpc::IChannelPtr masterChannel,
    NTransactionClient::ITransactionPtr transaction,
    NChunkClient::IBlockCachePtr blockCache,
    const NYPath::TRichYPath& richPath,
    const TNullable<i64>& offset,
    const TNullable<i64>& length)
{
    return Reader_->AsyncOpen(
        config,
        masterChannel,
        transaction,
        blockCache,
        richPath,
        offset,
        length
    ).Apply(BIND(&TThis::ReadBlock, MakeStrong(this)));
}

TAsyncError TDownloadSession::ReadBlock(TError error)
{
    RETURN_FUTURE_IF_ERROR(error, TError);
    return Reader_->AsyncRead().Apply(BIND(&TThis::WriteBlock, MakeStrong(this)));
}

TAsyncError TDownloadSession::WriteBlock(TErrorOr<TSharedRef> blockOrError)
{
    if (!blockOrError.IsOK()) {
        return MakeFuture(TError(blockOrError));
    }

    auto block = blockOrError.GetValue();

    if (block.Size() == 0) {
        return MakeFuture(TError());
    }

    if (!Output_->Write(block.Begin(), block.Size())) {
        return Output_->GetWriteFuture().Apply(BIND(&TThis::ReadBlock, MakeStrong(this)));
    }
    else {
        return ReadBlock(TError());
    }
}

////////////////////////////////////////////////////////////////////////////////

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

TUploadSession::TUploadSession(
    TAsyncWriterPtr writer,
    IAsyncInputStreamPtr input,
    size_t blockSize)
        : Writer_(writer)
        , Input_(input)
        , Buffer_(TSharedRef::Allocate(blockSize))
{ }

TAsyncError TUploadSession::Execute()
{
    return Writer_->AsyncOpen().Apply(BIND(&TThis::ReadBlock, MakeStrong(this)));
}

TAsyncError TUploadSession::ReadBlock(TError error)
{
    RETURN_FUTURE_IF_ERROR(error, TError);

    TAsyncError future =
          Input_->Read(Buffer_.Begin(), Buffer_.Size())
          ? MakeFuture(TError())
          : Input_->GetReadFuture();

    return future.Apply(BIND(&TThis::WriteBlock, MakeStrong(this)));
}

TAsyncError TUploadSession::WriteBlock(TError error)
{
    RETURN_FUTURE_IF_ERROR(error, TError);

    size_t length = Input_->GetReadLength();
    if (length == 0) {
        return Writer_->AsyncClose();
    }

    return Writer_->AsyncWrite(TRef(Buffer_.Begin(), length)).Apply(BIND(&TThis::ReadBlock, MakeStrong(this)));
}

////////////////////////////////////////////////////////////////////////////////

void TDownloadCommand::DoExecute()
{
    auto config = UpdateYsonSerializable(
        Context->GetConfig()->FileReader,
        Request->FileReader);

    Session_ = New<TDownloadSession>(
        New<TAsyncReader>(),
        Context->GetRequest()->OutputStream);

    auto result = Session_->Execute(
        config,
        Context->GetMasterChannel(),
        GetTransaction(EAllowNullTransaction::Yes, EPingTransaction::Yes),
        Context->GetBlockCache(),
        Request->Path,
        Request->Offset,
        Request->Length);

    CheckAndReply(result);
}

TDownloadCommand::~TDownloadCommand()
{ }

TDownloadCommand::TDownloadCommand()
{ }

//////////////////////////////////////////////////////////////////////////////////

void TUploadCommand::DoExecute()
{
    auto config = UpdateYsonSerializable(
        Context->GetConfig()->FileWriter,
        Request->FileWriter);

    auto writer = New<TAsyncWriter>(
        config,
        Context->GetMasterChannel(),
        GetTransaction(EAllowNullTransaction::Yes, EPingTransaction::Yes),
        Context->GetTransactionManager(),
        Request->Path);

    Session_ = New<TUploadSession>(
        writer,
        Context->GetRequest()->InputStream,
        config->BlockSize);

    CheckAndReply(Session_->Execute());
}

TUploadCommand::~TUploadCommand()
{ }

TUploadCommand::TUploadCommand()
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
