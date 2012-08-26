#pragma once

#include "public.h"

#include <ytlib/misc/nullable.h>

#include <ytlib/rpc/public.h>

#include <ytlib/file_client/public.h>

#include <ytlib/transaction_client/public.h>

#include <ytlib/chunk_client/public.h>

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
        const NTransactionClient::TTransactionId& transactionId);

    ~TErrorOutput() throw();

    NChunkClient::TChunkId GetChunkId() const;

private: 
    void DoWrite(const void* buf, size_t len);
    void DoFinish();

private:
    NFileClient::TFileWriterConfigPtr Config;
    NRpc::IChannelPtr MasterChannel;
    NTransactionClient::TTransactionId TransactionId;

    TAutoPtr<NFileClient::TFileChunkOutput> FileWriter;
};

////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
