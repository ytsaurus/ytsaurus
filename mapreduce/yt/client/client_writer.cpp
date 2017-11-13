#include "client_writer.h"

#include "retryful_writer.h"

#include <mapreduce/yt/interface/io.h>
#include <mapreduce/yt/common/helpers.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TClientWriter::TClientWriter(
    const TRichYPath& path,
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TMaybe<TFormat>& format,
    const TTableWriterOptions& options)
    : BlockWriter_(new TRetryfulWriter(
        auth,
        transactionId,
        GetWriteTableCommand(),
        format,
        path,
        BUFFER_SIZE,
        options))
{ }

size_t TClientWriter::GetStreamCount() const
{
    return 1;
}

IOutputStream* TClientWriter::GetStream(size_t tableIndex) const
{
    Y_UNUSED(tableIndex);
    return BlockWriter_.Get();
}

void TClientWriter::OnRowFinished(size_t)
{
    BlockWriter_->NotifyRowEnd();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
