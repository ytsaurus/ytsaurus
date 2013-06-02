#include "stdafx.h"
#include "file_reader.h"
#include "file_chunk_reader.h"
#include "config.h"
#include "private.h"

#include <ytlib/object_client/object_service_proxy.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>

#include <ytlib/transaction_client/transaction.h>

#include <ytlib/misc/sync.h>

#include <ytlib/chunk_client/chunk_replica.h>
#include <ytlib/chunk_client/chunk_spec.h>
#include <ytlib/chunk_client/schema.h>

#include <ytlib/node_tracker_client/node_directory.h>

namespace NYT {
namespace NFileClient {

using namespace NYTree;
using namespace NYPath;
using namespace NChunkClient;
using namespace NTransactionClient;
using namespace NNodeTrackerClient;

////////////////////////////////////////////////////////////////////////////////

TAsyncReader::TAsyncReader()
    : IsFirstBlock(true)
    , Size(0)
    , Logger(FileReaderLogger)
{ }

TAsyncError TAsyncReader::AsyncOpen(
    TFileReaderConfigPtr config,
    NRpc::IChannelPtr masterChannel,
    ITransactionPtr transaction,
    IBlockCachePtr blockCache,
    const TRichYPath& richPath,
    const TNullable<i64>& offset,
    const TNullable<i64>& length)
{
    YCHECK(config);
    YCHECK(masterChannel);
    YCHECK(blockCache);

    Logger.AddTag(Sprintf("Path: %s, TransactionId: %s",
        ~richPath.GetPath(),
        transaction ? ~ToString(transaction->GetId()) : ~ToString(NullTransactionId)));

    LOG_INFO("Opening file reader");

    LOG_INFO("Fetching file info");

    auto attributes = CreateEphemeralAttributes();

    i64 lowerLimit = offset ? offset.Get() : 0;
    if (offset) {
        NChunkClient::NProto::TReadLimit limit;
        limit.set_offset(offset.Get());
        attributes->SetYson("lower_limit", ConvertToYsonString(limit));
    }

    if (length) {
        NChunkClient::NProto::TReadLimit limit;
        limit.set_offset(lowerLimit + length.Get());
        attributes->SetYson("upper_limit", ConvertToYsonString(limit));
    }

    LOG_DEBUG("Fetching file path: %s", ~ToString(richPath));

    auto fetchReq = TFileYPathProxy::Fetch(richPath.Simplify().GetPath());
    ToProto(fetchReq->mutable_attributes(), *attributes);
    SetTransactionId(fetchReq, transaction);
    fetchReq->add_extension_tags(TProtoExtensionTag<NChunkClient::NProto::TMiscExt>::Value);

    NObjectClient::TObjectServiceProxy proxy(masterChannel);

    return proxy.Execute(fetchReq).Apply(
        BIND(&TThis::OnInfoFetched, MakeStrong(this), config, masterChannel, transaction, blockCache));
}

TAsyncError TAsyncReader::OnInfoFetched(
    TFileReaderConfigPtr config,
    NRpc::IChannelPtr masterChannel,
    ITransactionPtr transaction,
    IBlockCachePtr blockCache,
    TFileYPathProxy::TRspFetchPtr fetchRsp)
{
    if (!fetchRsp->IsOK()) {
        return MakeFuture(TError("Error fetching file info"));
    }

    TNodeDirectoryPtr nodeDirectory = New<TNodeDirectory>();
    nodeDirectory->MergeFrom(fetchRsp->node_directory());

    std::vector<NChunkClient::NProto::TChunkSpec> chunks =
        FromProto<NChunkClient::NProto::TChunkSpec>(fetchRsp->chunks());

    FOREACH(const auto& chunk, chunks) {
        i64 dataSize;
        GetStatistics(chunk, &dataSize);
        Size += dataSize;
    }

    auto provider = New<TFileChunkReaderProvider>(config);
    Reader = New<TFileChunkSequenceReader>(
        config,
        masterChannel,
        blockCache,
        nodeDirectory,
        std::move(chunks),
        provider);

    auto this_ = MakeStrong(this);
    return Reader->AsyncOpen().Apply(
        BIND([this, this_, transaction] (TError error) -> TError {
            if (!error.IsOK()) {
                return error;
            }
            if (transaction) {
                ListenTransaction(transaction);
            }
            LOG_INFO("File reader opened");
            return TError();
        })
    );
}

TFuture<TAsyncReader::TResult> TAsyncReader::AsyncRead()
{
    if (IsAborted()) {
        return MakeFuture<TAsyncReader::TResult>(TError("Transaction aborted"));
    }

    auto result = MakeFuture(TError());
    if (!IsFirstBlock && !Reader->FetchNext()) {
        result = Reader->GetReadyEvent();
    }

    if (IsFirstBlock) {
        IsFirstBlock = false;
    }

    auto this_ = MakeStrong(this);
    return result.Apply(BIND([this, this_] (TError error) -> TResult {
        if (!error.IsOK()) {
            return error;
        }
        auto* facade = Reader->GetFacade();
        return facade ? facade->GetBlock() : TSharedRef();
    }));
}

i64 TAsyncReader::GetSize() const
{
    return Size;
}

////////////////////////////////////////////////////////////////////////////////

TSyncReader::TSyncReader()
    : AsyncReader_(New<TAsyncReader>())
{ }


void TSyncReader::Open(
    TFileReaderConfigPtr config,
    NRpc::IChannelPtr masterChannel,
    ITransactionPtr transaction,
    IBlockCachePtr blockCache,
    const TRichYPath& richPath,
    const TNullable<i64>& offset,
    const TNullable<i64>& length)
{
    auto result = AsyncReader_->AsyncOpen(
        config,
        masterChannel,
        transaction,
        blockCache,
        richPath,
        offset,
        length).Get();

    THROW_ERROR_EXCEPTION_IF_FAILED(result);
}

TSharedRef TSyncReader::Read()
{
    auto result = AsyncReader_->AsyncRead().Get();
    THROW_ERROR_EXCEPTION_IF_FAILED(result);
    return result.GetValue();
}


i64 TSyncReader::GetSize() const
{
    return AsyncReader_->GetSize();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
