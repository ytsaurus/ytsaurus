#include "stdafx.h"
#include "file_reader.h"
#include "file_chunk_reader.h"
#include "config.h"
#include "private.h"
#include "file_ypath_proxy.h"

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

TFileReader::TFileReader()
    : IsFirstBlock(true)
    , Size(0)
    , Logger(FileReaderLogger)
{ }

TFileReader::~TFileReader()
{ }

void TFileReader::Open(
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

    auto fetchRsp = proxy.Execute(fetchReq).Get();
    THROW_ERROR_EXCEPTION_IF_FAILED(*fetchRsp, "Error fetching file info");

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

    Sync(~Reader, &TFileChunkSequenceReader::AsyncOpen);

    if (transaction) {
        ListenTransaction(transaction);
    }

    LOG_INFO("File reader opened");
}

TSharedRef TFileReader::Read()
{
    CheckAborted();

    if (IsFirstBlock) {
        IsFirstBlock = false;
    } else if (!Reader->FetchNext()) {
        Sync(~Reader, &TFileChunkSequenceReader::GetReadyEvent);
    }

    auto* facade = Reader->GetFacade();
    return facade ? facade->GetBlock() : TSharedRef();
}

i64 TFileReader::GetSize() const
{
    return Size;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
