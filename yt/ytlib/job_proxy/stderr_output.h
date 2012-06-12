#pragma once

#include "public.h"

// ToDo(psushin): use public.h
#include <ytlib/file_client/public.h>
#include <ytlib/chunk_server/public.h>
#include <ytlib/transaction_server/public.h>
#include <ytlib/rpc/public.h>
#include <misc/nullable.h>


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
        const NTransactionServer::TTransactionId& transactionId);

    ~TErrorOutput() throw();

    NChunkServer::TChunkId GetChunkId() const;

private: 
    void DoWrite(const void* buf, size_t len);
    void DoFinish();

private:
    NFileClient::TFileWriterConfigPtr Config;
    NRpc::IChannelPtr MasterChannel;
    NTransactionServer::TTransactionId TransactionId;

    TAutoPtr<NFileClient::TFileChunkOutput> FileWriter;
};

////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
