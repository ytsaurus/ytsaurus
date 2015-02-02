#pragma once

#include "public.h"

#include <core/misc/nullable.h>

#include <core/rpc/public.h>

#include <ytlib/file_client/public.h>

#include <ytlib/api/public.h>

#include <ytlib/transaction_client/public.h>

#include <ytlib/chunk_client/public.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////

// ToDo(psushin): rename to TDiagnosticStream(?)
class TErrorOutput
    : public TOutputStream
{
public:
    TErrorOutput(
        NApi::TFileWriterConfigPtr config,
        NRpc::IChannelPtr masterChannel,
        const NTransactionClient::TTransactionId& transactionId);

    TErrorOutput(
        NApi::TFileWriterConfigPtr config,
        NRpc::IChannelPtr masterChannel,
        const NTransactionClient::TTransactionId& transactionId,
        i64 maxSize);

    ~TErrorOutput() throw();

    NChunkClient::TChunkId GetChunkId() const;

private:
    void DoWrite(const void* buf, size_t len);
    void DoFinish();

private:
    NApi::TFileWriterConfigPtr Config;
    NRpc::IChannelPtr MasterChannel;
    NTransactionClient::TTransactionId TransactionId;
    const i64 MaxSize;

    bool IsClosed;

    std::unique_ptr<NFileClient::TFileChunkOutput> FileWriter;
};

////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
