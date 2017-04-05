#include "file_writer.h"

#include "helpers.h"

#include <mapreduce/yt/common/helpers.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TFileWriter::TFileWriter(
    const TRichYPath& path,
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TFileWriterOptions& options)
    : BlockWriter_(
        auth,
        transactionId,
        GetWriteFileCommand(),
        DSF_BYTES,
        "",
        path,
        BUFFER_SIZE,
        options)
{ }

TFileWriter::~TFileWriter()
{
    try {
        Finish();
    } catch (...) {
    }
}

void TFileWriter::DoWrite(const void* buf, size_t len)
{
    BlockWriter_.Write(buf, len);
    BlockWriter_.NotifyRowEnd();
}

void TFileWriter::DoFinish()
{
    BlockWriter_.Finish();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
