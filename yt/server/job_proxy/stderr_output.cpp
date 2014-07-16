#include "stdafx.h"
#include "stderr_output.h"
#include "private.h"

#include <ytlib/file_client/config.h>
#include <ytlib/file_client/file_chunk_output.h>

#include <ytlib/transaction_client/transaction_ypath_proxy.h>

#include <ytlib/security_client/public.h>

#include <core/rpc/channel.h>

namespace NYT {
namespace NJobProxy {

using namespace NFileClient;
using namespace NApi;
using namespace NRpc;
using namespace NTransactionClient;
using namespace NChunkClient;

////////////////////////////////////////////////////////////////////

static const auto& Logger = JobProxyLogger;

////////////////////////////////////////////////////////////////////

TErrorOutput::TErrorOutput(
    TFileWriterConfigPtr config,
    IChannelPtr masterChannel,
    const TTransactionId& transactionId,
    i64 maxSize)
    : Config(config)
    , MasterChannel(masterChannel)
    , TransactionId(transactionId)
    , MaxSize(maxSize)
    , IsClosed(false)
{ }

TErrorOutput::~TErrorOutput() throw()
{ }

void TErrorOutput::DoWrite(const void* buf, size_t len)
{
    if (!FileWriter) {
        LOG_DEBUG("Opening stderr stream");

        auto options = New<TMultiChunkWriterOptions>();
        options->Account = NSecurityClient::SysAccountName;
        options->ReplicationFactor = 1;
        options->ChunksVital = false;

        FileWriter.reset(new TFileChunkOutput(
            Config,
            options,
            MasterChannel,
            TransactionId));
        FileWriter->Open();

        LOG_DEBUG("Stderr stream opened");
    }

    if (FileWriter->GetSize() > MaxSize)
        return;

    FileWriter->Write(buf, len);
}

void TErrorOutput::DoFinish()
{
    if (!FileWriter)
        return;

    FileWriter->Finish();
    IsClosed = true;;
}

TChunkId TErrorOutput::GetChunkId() const
{
    return IsClosed ? FileWriter->GetChunkId() : NullChunkId;
}

////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
