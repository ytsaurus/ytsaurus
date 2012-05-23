#include "stdafx.h"
#include "file_reader.h"
#include "config.h"

#include <ytlib/file_server/file_ypath_proxy.h>
#include <ytlib/cypress/cypress_ypath_proxy.h>
#include <ytlib/misc/string.h>
#include <ytlib/misc/sync.h>

namespace NYT {
namespace NFileClient {

using namespace NObjectServer;
using namespace NCypress;
using namespace NYTree;
using namespace NTransactionClient;
using namespace NFileServer;
using namespace NChunkClient;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

TFileReader::TFileReader(
    TFileReaderConfigPtr config,
    NRpc::IChannelPtr masterChannel,
    ITransaction* transaction,
    IBlockCache* blockCache,
    const TYPath& path)
    : TFileReaderBase(config, masterChannel, blockCache)
    , Transaction(transaction)
    , Path(path)
{
    Logger.AddTag(Sprintf("Path: %s, TransactionId: %s",
        ~Path,
        transaction ? ~transaction->GetId().ToString() : ~NullTransactionId.ToString()));
}

void TFileReader::Open()
{
    LOG_INFO("Opening file reader");

    LOG_INFO("Fetching file info");
    auto fetchReq = TFileYPathProxy::Fetch(WithTransaction(Path, Transaction ? Transaction->GetId() : NullTransactionId));
    auto fetchRsp = Proxy.Execute(fetchReq).Get();
    if (!fetchRsp->IsOK()) {
        LOG_ERROR_AND_THROW(yexception(), "Error fetching file info\n%s",
            ~fetchRsp->GetError().ToString());
    }
    auto chunkId = TChunkId::FromProto(fetchRsp->chunk_id());
    auto holderAddresses = FromProto<Stroka>(fetchRsp->node_addresses());
    LOG_INFO("File info received (ChunkId: %s, HolderAddresses: [%s])",
        ~chunkId.ToString(),
        ~JoinToString(holderAddresses));

    TFileReaderBase::Open(chunkId, holderAddresses);

    if (Transaction) {
        ListenTransaction(~Transaction);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
