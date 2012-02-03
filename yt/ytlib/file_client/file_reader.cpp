#include "stdafx.h"
#include "file_reader.h"
#include "file_chunk_meta.pb.h"

#include <ytlib/misc/string.h>
#include <ytlib/misc/sync.h>
#include <ytlib/file_server/file_ypath_proxy.h>

namespace NYT {
namespace NFileClient {

using namespace NCypress;
using namespace NYTree;
using namespace NTransactionClient;
using namespace NFileServer;
using namespace NChunkClient;
using namespace NTransactionClient;
using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

TFileReader::TFileReader(
    TConfig* config,
    NRpc::IChannel* masterChannel,
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
    auto fetchRsp = Proxy.Execute(~fetchReq)->Get();
    if (!fetchRsp->IsOK()) {
        LOG_ERROR_AND_THROW(yexception(), "Error fetching file info\n%s",
            ~fetchRsp->GetError().ToString());
    }
    auto chunkId = TChunkId::FromProto(fetchRsp->chunk_id());
    auto holderAddresses = FromProto<Stroka>(fetchRsp->holder_addresses());
    FileName = fetchRsp->file_name();
    Executable = fetchRsp->executable();
    LOG_INFO("File info received (ChunkId: %s, FileName: %s, Executable: %s, HolderAddresses: [%s])",
        ~chunkId.ToString(),
        ~FileName,
        ~ToString(Executable),
        ~JoinToString(holderAddresses));

    TFileReaderBase::Open(chunkId, holderAddresses);

    if (Transaction) {
        ListenTransaction(~Transaction);
    }
}

Stroka TFileReader::GetFileName() const
{
    VERIFY_THREAD_AFFINITY(Client);
    // YASSERT(IsOpen);

    return FileName;
}

bool TFileReader::IsExecutable()
{
    VERIFY_THREAD_AFFINITY(Client);
    // YASSERT(IsOpen);

    return Executable;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
