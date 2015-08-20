#include "file_writer.h"

#include <mapreduce/yt/common/helpers.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TFileWriter::TFileWriter(
    const TRichYPath& path,
    const TAuth& auth,
    const TTransactionId& transactionId)
    : BlockWriter_(
        auth,
        transactionId,
        GetWriteFileCommand(),
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
