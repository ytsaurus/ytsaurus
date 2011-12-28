#pragma once
#include "common.h"
#include "chunk_sequence_reader.h"

#include "../rpc/channel.h"
#include "../transaction_client/transaction.h"
#include "../cypress/cypress_service_proxy.h"
#include "../table_server/table_ypath_proxy.h"
#include "../chunk_client/block_cache.h"
#include "../logging/tagged_logger.h"

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TTableReader
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TTableReader> TPtr;

    struct TConfig
        : public TConfigurable
    {
        typedef TIntrusivePtr<TConfig> TPtr;

        TDuration MasterRpcTimeout;
        TChunkSequenceReader::TConfig::TPtr ChunkSequenceReader;

        TConfig()
        {
            Register("cypress_rpc_timeout", MasterRpcTimeout).Default(TDuration::Seconds(5));
            Register("chunk_sequence_reader", ChunkSequenceReader).DefaultNew();
        }
    };

    TTableReader(
        TConfig* config,
        NRpc::IChannel* masterChannel,
        NTransactionClient::ITransaction* transaction,
        NChunkClient::IBlockCache* blockCache,
        const TChannel& readChannel,
        const NYTree::TYPath& path);

    bool NextRow();
    bool NextColumn();
    TColumn GetColumn() const;
    TValue GetValue() const;
    void Close();

private:
    void OnAborted();

    TConfig::TPtr Config;
    TChunkSequenceReader::TPtr Reader;
    NCypress::TNodeId NodeId;
    NTransactionClient::ITransaction::TPtr Transaction;
    NLog::TTaggedLogger Logger;

    IAction::TPtr OnAborted_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
