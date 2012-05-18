#pragma once

#include "public.h"

// ToDo(psushin): use public.h
#include <ytlib/file_client/public.h>
#include <ytlib/chunk_server/public.h>
#include <ytlib/transaction_server/public.h>
#include <ytlib/rpc/public.h>


namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////

class TErrorOutput
    : public TOutputStream
{
public:
    TErrorOutput(
        NFileClient::TFileWriterConfigPtr config, 
        NRpc::IChannelPtr masterChannel,
        const NTransactionServer::TTransactionId& transactionId,
        const NChunkServer::TChunkListId& chunkListId);

    ~TErrorOutput() throw();

private: 
    void DoWrite(const void* buf, size_t len);
    void DoFinish();

private:
    NFileClient::TFileWriterBasePtr FileWriter;
    NRpc::IChannelPtr MasterChannel;
    NTransactionServer::TTransactionId TransactionId;
    NChunkServer::TChunkListId ChunkListId;
};

////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
