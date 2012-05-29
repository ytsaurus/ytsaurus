#include "stdafx.h"
#include "stderr_output.h"

#include <ytlib/file_client/config.h>
#include <ytlib/file_client/file_chunk_output.h>
#include <ytlib/chunk_server/chunk_list_ypath_proxy.h>
#include <ytlib/transaction_server/transaction_ypath_proxy.h>
#include <ytlib/rpc/channel.h>

namespace NYT {
namespace NJobProxy {

using namespace NFileClient;
using namespace NRpc;
using namespace NTransactionServer;
using namespace NChunkServer;

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
        FileWriter = new TFileChunkOutput(Config, MasterChannel, TransactionId);
    }

    FileWriter->Write(buf, len);
}

void TErrorOutput::DoFinish() 
{
    if (FileWriter) {
        FileWriter->Finish();
    }
}

TNullable<TChunkId> TErrorOutput::GetChunkId() const
{
    if (!FileWriter) {
        return Null;
    }

    return FileWriter->GetChunkId();
}

////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
