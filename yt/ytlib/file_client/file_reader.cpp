#include "stdafx.h"
#include "file_reader.h"
#include "config.h"

#include <ytlib/misc/string.h>
#include <ytlib/misc/sync.h>

#include <ytlib/file_client/file_ypath_proxy.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>

#include <ytlib/transaction_client/transaction.h>

namespace NYT {
namespace NFileClient {

using namespace NCypressClient;
using namespace NYTree;
using namespace NYPath;
using namespace NTransactionClient;
using namespace NChunkClient;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

TFileReader::TFileReader(
    TFileReaderConfigPtr config,
    NRpc::IChannelPtr masterChannel,
    ITransactionPtr transaction,
    IBlockCachePtr blockCache,
    const TRichYPath& richPath)
    : TFileReaderBase(config, masterChannel, blockCache)
    , Transaction(transaction)
    , RichPath(richPath)
{
    Logger.AddTag(Sprintf("Path: %s, TransactionId: %s",
        ~RichPath.GetPath(),
        transaction ? ~transaction->GetId().ToString() : ~NullTransactionId.ToString()));
}

void TFileReader::Open()
{
    LOG_INFO("Opening file reader");

    LOG_INFO("Fetching file info");
    auto fetchReq = TFileYPathProxy::FetchFile(RichPath);
    SetTransactionId(fetchReq, Transaction);
    auto fetchRsp = Proxy.Execute(fetchReq).Get();
    THROW_ERROR_EXCEPTION_IF_FAILED(*fetchRsp, "Error fetching file info");

    auto chunkId = TChunkId::FromProto(fetchRsp->chunk_id());
    auto addresses = FromProto<Stroka>(fetchRsp->node_addresses());
    LOG_INFO("File info received (ChunkId: %s, Addresses: [%s])",
        ~chunkId.ToString(),
        ~JoinToString(addresses));

    TFileReaderBase::Open(chunkId, addresses);

    if (Transaction) {
        ListenTransaction(Transaction);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
