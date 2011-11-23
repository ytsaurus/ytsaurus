#pragma once

#include "common.h"

#include "../misc/config.h"
#include "../rpc/channel.h"
#include "../transaction_client/transaction.h"
#include "../cypress/cypress_service_rpc.h"
#include "../chunk_client/sequential_reader.h"
#include "../chunk_client/remote_reader.h"

namespace NYT {
namespace NFileClient {

////////////////////////////////////////////////////////////////////////////////

class TFileReader
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TFileReader> TPtr;

    struct TConfig
        : TConfigBase
    {
        TConfig()
        {
            Register("master_rpc_timeout", MasterRpcTimeout).Default(TDuration::MilliSeconds(5000));
            Register("sequential_reader", SequentialReader);
            Register("remote_reader", RemoteReader);

            SetDefaults();
        }

        TDuration MasterRpcTimeout;
        NChunkClient::TSequentialReader::TConfig SequentialReader;
        NChunkClient::TRemoteReader::TConfig RemoteReader;
    };

    TFileReader(
        const TConfig& config,
        NRpc::IChannel* masterChannel,
        NTransactionClient::ITransaction* transaction,
        NYTree::TYPath path);

    //! Returns the size of the file.
    i64 GetSize() const;

    //! Reads the next block.
    /*!
     *  \returns The next block or NULL reference is the end is reached.
     */
    TSharedRef Read();

    //! Closes the reader.
    void Close();

private:
    TConfig Config;
    NRpc::IChannel::TPtr MasterChannel;
    NTransactionClient::ITransaction::TPtr Transaction;
    NYTree::TYPath Path;
    bool Closed;
    volatile bool Aborted;

    TAutoPtr<NCypress::TCypressServiceProxy> CypressProxy;
    NChunkClient::TSequentialReader::TPtr SequentialReader;
    NChunkClient::TChunkId ChunkId;
    i32 BlockCount;
    i32 BlockIndex;
    i64 Size;

    IAction::TPtr OnAborted_;

    void CheckAborted();
    void OnAborted();

    void Finish();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT