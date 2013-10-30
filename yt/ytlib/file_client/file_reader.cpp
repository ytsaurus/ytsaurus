#include "stdafx.h"
#include "file_reader.h"
#include "file_chunk_reader.h"
#include "config.h"
#include "private.h"

#include <ytlib/object_client/object_service_proxy.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>

#include <ytlib/transaction_client/transaction.h>

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
using namespace NObjectClient;
using namespace NCypressClient;

////////////////////////////////////////////////////////////////////////////////

TAsyncReader::TAsyncReader(
    TFileReaderConfigPtr config,
    NRpc::IChannelPtr masterChannel,
    NChunkClient::IBlockCachePtr blockCache,
    NTransactionClient::ITransactionPtr transaction,
    const TRichYPath& richPath,
    const TNullable<i64>& offset,
    const TNullable<i64>& length)
    : Config(config)
    , MasterChannel(masterChannel)
    , BlockCache(blockCache)
    , Transaction(transaction)
    , RichPath(richPath.Simplify())
    , Offset(offset)
    , Length(length)
    , IsFirstBlock(true)
    , Size(0)
    , Logger(FileReaderLogger)
{
    YCHECK(config);
    YCHECK(masterChannel);
    YCHECK(blockCache);

    Logger.AddTag(Sprintf("Path: %s, TransactionId: %s",
        ~RichPath.GetPath(),
        transaction ? ~ToString(transaction->GetId()) : ~ToString(NullTransactionId)));
}

TAsyncReader::~TAsyncReader()
{ }

TAsyncError TAsyncReader::AsyncOpen()
{
    LOG_INFO("Opening file reader");

    LOG_INFO("Fetching file info");

    auto attributes = CreateEphemeralAttributes();

    i64 lowerLimit = Offset.Get(0);
    if (Offset) {
        NChunkClient::NProto::TReadLimit limit;
        limit.set_offset(*Offset);
        attributes->SetYson("lower_limit", ConvertToYsonString(limit));
    }

    if (Length) {
        NChunkClient::NProto::TReadLimit limit;
        limit.set_offset(lowerLimit + *Length);
        attributes->SetYson("upper_limit", ConvertToYsonString(limit));
    }

    auto fetchReq = TFileYPathProxy::Fetch(RichPath.GetPath());
    ToProto(fetchReq->mutable_attributes(), *attributes);
    SetTransactionId(fetchReq, Transaction);
    SetSuppressAccessTracking(fetchReq, Config->SuppressAccessTracking);
    fetchReq->add_extension_tags(TProtoExtensionTag<NChunkClient::NProto::TMiscExt>::Value);

    TObjectServiceProxy proxy(MasterChannel);
    return proxy.Execute(fetchReq).Apply(
        BIND(&TThis::OnInfoFetched, MakeStrong(this)));
}

TAsyncError TAsyncReader::OnInfoFetched(TFileYPathProxy::TRspFetchPtr fetchRsp)
{
    if (!fetchRsp->IsOK()) {
        return MakeFuture(TError("Error fetching file info")
            << *fetchRsp);
    }

    auto nodeDirectory = New<TNodeDirectory>();
    nodeDirectory->MergeFrom(fetchRsp->node_directory());

    std::vector<NChunkClient::NProto::TChunkSpec> chunks =
        FromProto<NChunkClient::NProto::TChunkSpec>(fetchRsp->chunks());

    FOREACH(const auto& chunk, chunks) {
        i64 dataSize;
        GetStatistics(chunk, &dataSize);
        Size += dataSize;
    }

    auto provider = New<TFileChunkReaderProvider>(Config);
    Reader = New<TReader>(
        Config,
        MasterChannel,
        BlockCache,
        nodeDirectory,
        std::move(chunks),
        provider);

    auto this_ = MakeStrong(this);
    return Reader->AsyncOpen().Apply(
        BIND([this, this_] (TError error) -> TError {
            if (!error.IsOK()) {
                return error;
            }
            if (Transaction) {
                ListenTransaction(Transaction);
            }
            LOG_INFO("File reader opened");
            return TError();
        })
    );
}

TFuture<TAsyncReader::TReadResult> TAsyncReader::AsyncRead()
{
    if (IsAborted()) {
        return MakeFuture<TAsyncReader::TReadResult>(TError("Transaction aborted"));
    }

    auto result = MakeFuture(TError());
    if (!IsFirstBlock && !Reader->FetchNext()) {
        result = Reader->GetReadyEvent();
    }

    if (IsFirstBlock) {
        IsFirstBlock = false;
    }

    auto this_ = MakeStrong(this);
    return result.Apply(BIND([this, this_] (TError error) -> TReadResult {
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

} // namespace NFileClient
} // namespace NYT
