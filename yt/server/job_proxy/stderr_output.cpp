#include "stdafx.h"
#include "stderr_output.h"
#include "private.h"

#include <ytlib/file_client/config.h>
#include <ytlib/file_client/file_chunk_output.h>

#include <ytlib/chunk_client/chunk_list_ypath_proxy.h>

#include <ytlib/transaction_client/transaction_ypath_proxy.h>

#include <ytlib/rpc/channel.h>

namespace NYT {
namespace NJobProxy {

using namespace NFileClient;
using namespace NRpc;
using namespace NTransactionClient;
using namespace NChunkServer;

////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = JobProxyLogger;

////////////////////////////////////////////////////////////////////

TErrorOutput::TErrorOutput(
    TFileWriterConfigPtr config, 
    IChannelPtr masterChannel,
    const TTransactionId& transactionId)
    : Config(config)
    , MasterChannel(masterChannel)
    , TransactionId(transactionId)
{ }

TErrorOutput::~TErrorOutput() throw()
{ }

void TErrorOutput::DoWrite(const void* buf, size_t len)
{
    if (!FileWriter) {
        LOG_DEBUG("Opening stderr stream");

        FileWriter = new TFileChunkOutput(Config, MasterChannel, TransactionId);

        try {
            FileWriter->Open();
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Error opening stderr stream");
            // TOOD(babenko): extract constant
            _exit(777);
        }

        LOG_DEBUG("Stderr stream opened");
    }

    try {
        FileWriter->Write(buf, len);
    } catch (const std::exception& ex) {
        LOG_ERROR(ex, "Error writing to stderr stream");
        // TOOD(babenko): extract constant
        _exit(777);
    }
}

void TErrorOutput::DoFinish() 
{
    if (!FileWriter)
        return;

    try {
        FileWriter->Finish();
    } catch (const std::exception& ex) {
        LOG_ERROR(ex, "Error closing stderr stream");
        // TOOD(babenko): extract constant
        _exit(777);
    }
}

TChunkId TErrorOutput::GetChunkId() const
{
    return ~FileWriter ? FileWriter->GetChunkId() : NullChunkId;
}

////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
