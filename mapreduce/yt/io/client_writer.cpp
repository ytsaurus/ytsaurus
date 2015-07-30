#include "client_writer.h"

#include "block_writer.h"

#include <mapreduce/yt/common/helpers.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TClientWriter::TClientWriter(
    const TRichYPath& path,
    const Stroka& serverName,
    const TTransactionId& transactionId,
    EDataStreamFormat format)
    : Path_(path)
    , ServerName_(serverName)
    , TransactionId_(transactionId)
    , Format_(format)
    , BlockWriter_(new TBlockWriter(
        serverName,
        transactionId,
        "write",
        format,
        path,
        BUFFER_SIZE))
{ }

size_t TClientWriter::GetStreamCount() const
{
    return 1;
}

TOutputStream* TClientWriter::GetStream(size_t tableIndex)
{
    UNUSED(tableIndex);
    return BlockWriter_.Get();
}

void TClientWriter::OnRowFinished(size_t)
{
    BlockWriter_->DoFlushIfNeeded();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
