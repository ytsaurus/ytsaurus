#pragma once
#include "common.h"
#include "chunk_set_writer.h"

#include "../rpc/channel.h"
#include "../transaction_client/transaction.h"
#include "../cypress/cypress_service_rpc.h"
#include "../table_server/table_service_rpc.h"

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

//! Used only on client-side for writing to table.
//! Synchronous API only.
class TTableWriter
    : public TNonCopyable
{
public:
    struct TConfig {
        TDuration RpcTimeout;
        TChunkSetWriter::TConfig ChunkSetConfig;
    };

    TTableWriter(
        const TConfig& config,
        NRpc::IChannel::TPtr masterChannel,
        NTransactionClient::ITransaction::TPtr transaction,
        ICodec* codec,
        const TSchema& schema,
        const Stroka& ypath);

    void Write(const TColumn& column, TValue value);
    void EndRow();
    void Close();
    void Cancel();

private:
    bool NodeExists(const Stroka& nodePath);
    void CreateTableNode(const Stroka& nodePath);

    typedef NCypress::TCypressServiceProxy TCypressProxy;
    typedef NTableServer::TTableServiceProxy TTableProxy;

    const TConfig Config;
    const Stroka TablePath;
    Stroka NodeId;
    NTransactionClient::ITransaction::TPtr Transaction;
    NRpc::IChannel::TPtr MasterChannel;
    TChunkSetWriter::TPtr Writer;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
