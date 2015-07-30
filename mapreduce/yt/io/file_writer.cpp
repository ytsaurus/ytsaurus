#include "file_writer.h"

#include <mapreduce/yt/common/helpers.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TFileWriter::TFileWriter(
    const TRichYPath& path,
    const Stroka& serverName,
    const TTransactionId& transactionId)
    : Path_(path)
    , ServerName_(serverName)
    , TransactionId_(transactionId)
    , BlockWriter_(
        serverName,
        transactionId,
        "upload",
        DSF_BYTES,
        path,
        BUFFER_SIZE)
{ }

void TFileWriter::DoWrite(const void* buf, size_t len)
{
    BlockWriter_.Write(buf, len);
    BlockWriter_.DoFlushIfNeeded();
}

void TFileWriter::DoFinish()
{
    BlockWriter_.Finish();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
